use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};

pub enum DiskCommand {
    WriteBlock {
        file_path: PathBuf,
        offset: u64,
        data: bytes::Bytes,
        reply: oneshot::Sender<Result<(), String>>,
    },
    ReadBlock {
        file_path: PathBuf,
        offset: u64,
        length: usize,
        reply: oneshot::Sender<Result<bytes::Bytes, String>>,
    },
}

pub struct DiskActor {
    receiver: mpsc::Receiver<DiskCommand>,
}

impl DiskActor {
    pub fn spawn(buffer_size: usize) -> (mpsc::Sender<DiskCommand>, tokio::task::JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(buffer_size);
        let actor = DiskActor { receiver };
        
        let handle = tokio::task::spawn_blocking(move || {
            actor.run_loop();
        });
        
        (sender, handle)
    }

    fn run_loop(mut self) {
        while let Some(command) = self.receiver.blocking_recv() {
            match command {
                DiskCommand::WriteBlock { file_path, offset, data, reply } => {
                    let res = Self::handle_write(file_path, offset, &data);
                    let _ = reply.send(res);
                }
                DiskCommand::ReadBlock { file_path, offset, length, reply } => {
                    let res = Self::handle_read(file_path, offset, length);
                    let _ = reply.send(res);
                }
            }
        }
    }

    fn handle_write(path: PathBuf, offset: u64, data: &[u8]) -> Result<(), String> {
        // Ensure directory exists
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|e| format!("Failed to open file for writing: {}", e))?;
            
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| format!("Seek failed: {}", e))?;
            
        file.write_all(data)
            .map_err(|e| format!("Write failed: {}", e))?;
            
        Ok(())
    }

    fn handle_read(path: PathBuf, offset: u64, length: usize) -> Result<bytes::Bytes, String> {
        let mut file = File::open(&path)
            .map_err(|e| format!("Failed to open file for reading: {}", e))?;
            
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| format!("Seek failed: {}", e))?;
            
        let mut buf = vec![0u8; length];
        file.read_exact(&mut buf)
            .map_err(|e| format!("Read failed: {}", e))?;
            
        Ok(bytes::Bytes::from(buf))
    }
}
