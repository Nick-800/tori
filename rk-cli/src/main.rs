use clap::{Parser, Subcommand};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    widgets::{Block, Borders, Paragraph, Gauge},
    Terminal,
};
use std::io;
use torrent_proto::torrent_service_client::TorrentServiceClient;
use torrent_proto::{AddTorrentRequest, ListTorrentsRequest, StatusRequest};

pub mod torrent_proto {
    tonic::include_proto!("torrent");
}

#[derive(Parser)]
#[command(name = "rk")]
#[command(about = "Rektorrent CLI & TUI Controller")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Add { target: String, magnet: Option<String> },
    List,
    Tui,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    let mut client = TorrentServiceClient::connect("http://[::1]:50051").await?;

    match args.command.unwrap_or(Commands::Tui) {
        Commands::Add { target, magnet } => {
            let res = client.add_torrent(AddTorrentRequest { target, magnet: magnet.unwrap_or_default(), sequential: false }).await?;
            println!("Torrent added. InfoHash: {}", res.into_inner().info_hash);
        }
        Commands::List => {
            let res = client.list_torrents(ListTorrentsRequest {}).await?;
            for torrent in res.into_inner().torrents {
                println!(
                    "- {}: [{}] Progress: {:.2}%",
                    torrent.name,
                    torrent.info_hash,
                    torrent.progress * 100.0
                );
            }
        }
        Commands::Tui => {
            run_tui(&mut client).await?;
        }
    }

    Ok(())
}

async fn run_tui(client: &mut TorrentServiceClient<tonic::transport::Channel>) -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut stream = client.stream_status(StatusRequest {}).await?.into_inner();

    loop {
        // Read the latest update from stream
        let update = tokio::select! {
            next = stream.message() => {
                match next {
                    Ok(Some(status)) => status,
                    _ => break, // Stream closed or error
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {
                continue;
            }
        };

        terminal.draw(|f| {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Min(5),
                ].as_ref())
                .split(f.size());

            // Header block
            let header = Paragraph::new(" Rektorrent Active Sync Session Dashboard (Press 'q' to Quit) ")
                .block(Block::default().borders(Borders::ALL));
            f.render_widget(header, chunks[0]);

            // Body rendering lists with gauges
            if update.torrents.is_empty() {
                let info = Paragraph::new("No torrents active. Try adding a torrent using the CLI.")
                    .block(Block::default().borders(Borders::ALL));
                f.render_widget(info, chunks[1]);
            } else {
                let torrent = &update.torrents[0];
                let info_text = format!(
                    "Name: {}\nHash: {}\nDownload Speed: {:.2} KB/s\nUpload Speed: {:.2} KB/s\nPeers: {}",
                    torrent.name,
                    torrent.info_hash,
                    torrent.download_speed as f64 / 1024.0,
                    torrent.upload_speed as f64 / 1024.0,
                    torrent.peer_count
                );
                
                let inner_layout = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Length(6),
                        Constraint::Length(3),
                    ].as_ref())
                    .split(chunks[1]);

                let details = Paragraph::new(info_text).block(Block::default().title(" Torrent Info ").borders(Borders::ALL));
                f.render_widget(details, inner_layout[0]);

                let gauge = Gauge::default()
                    .block(Block::default().title(" Progress ").borders(Borders::ALL))
                    .gauge_style(ratatui::style::Style::default().fg(ratatui::style::Color::Cyan))
                    .percent((torrent.progress * 100.0) as u16);
                f.render_widget(gauge, inner_layout[1]);
            }
        })?;

        // Handle quick keyboard exit checks without blocking the draw rate
        if event::poll(std::time::Duration::from_millis(10))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    break;
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
    Ok(())
}
