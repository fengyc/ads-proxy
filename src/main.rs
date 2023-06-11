use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{ensure, Result};
use byteorder::{BigEndian, ReadBytesExt};
use clap::Parser;
use env_logger::Env;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct AmsNetId(pub [u8; 6]);

impl AmsNetId {
    pub const SIZE: usize = 6;
}

impl Display for AmsNetId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = self.0.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(".");
        write!(f, "{}", s)
    }
}

impl<'a> TryFrom<&'a [u8]> for AmsNetId {
    type Error = anyhow::Error;

    fn try_from(value: &'a [u8]) -> std::result::Result<Self, Self::Error> {
        ensure!(value.len() >= AmsNetId::SIZE, "invalid AmsNetId");
        let mut net_id = AmsNetId::default();
        net_id.0.copy_from_slice(value);
        Ok(net_id)
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct AmsAddr(pub AmsNetId, pub u16);

impl Display for AmsAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0, self.1)
    }
}

pub const AMS_TCP_HEADER_SIZE: usize = 6;

pub struct AmsTcpHeaderSlice<'a> {
    pub data: &'a [u8],
}

impl AmsTcpHeaderSlice<'_> {
    pub const SIZE: usize = AMS_TCP_HEADER_SIZE;

    pub fn reserved(&self) -> u16 {
        let mut rdr = Cursor::new(&self.data[0..2]);
        ReadBytesExt::read_u16::<BigEndian>(&mut rdr).unwrap()
    }

    pub fn length(&self) -> u32 {
        let mut rdr = Cursor::new(&self.data[2..6]);
        ReadBytesExt::read_u32::<BigEndian>(&mut rdr).unwrap()
    }
}

impl<'a> TryFrom<&'a [u8]> for AmsTcpHeaderSlice<'a> {
    type Error = anyhow::Error;

    fn try_from(value: &'a [u8]) -> std::result::Result<Self, Self::Error> {
        ensure!(value.len() >= AmsTcpHeaderSlice::SIZE, "invalid AmsTcpHeaderSlice");
        Ok(AmsTcpHeaderSlice { data: value })
    }
}

impl<'a> Display for AmsTcpHeaderSlice<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "reserved={}, length={}", self.reserved(), self.length())
    }
}

pub const AMS_HEADER_SIZE: usize = 32;

pub struct AmsHeaderSlice<'a> {
    pub data: &'a [u8],
}

impl AmsHeaderSlice<'_> {
    pub const SIZE: usize = AMS_HEADER_SIZE;

    fn read_u16(&self, offset: usize) -> u16 {
        let mut rdr = Cursor::new(&self.data[offset..offset + 2]);
        ReadBytesExt::read_u16::<BigEndian>(&mut rdr).unwrap()
    }

    fn read_u32(&self, offset: usize) -> u32 {
        let mut rdr = Cursor::new(&self.data[offset..offset + 4]);
        ReadBytesExt::read_u32::<BigEndian>(&mut rdr).unwrap()
    }

    pub fn target_net_id(&self) -> AmsNetId {
        AmsNetId::try_from(&self.data[..6]).unwrap()
    }

    pub fn target_port(&self) -> u16 {
        self.read_u16(6)
    }

    pub fn source_net_id(&self) -> AmsNetId {
        AmsNetId::try_from(&self.data[8..14]).unwrap()
    }

    pub fn source_port(&self) -> u16 {
        self.read_u16(14)
    }

    pub fn command_id(&self) -> u16 {
        self.read_u16(16)
    }

    pub fn state_flags(&self) -> u16 {
        self.read_u16(18)
    }

    pub fn length(&self) -> u32 {
        self.read_u32(20)
    }

    pub fn error_code(&self) -> u32 {
        self.read_u32(24)
    }

    pub fn invoke_id(&self) -> u32 {
        self.read_u32(28)
    }
}

impl<'a> TryFrom<&'a [u8]> for AmsHeaderSlice<'a> {
    type Error = anyhow::Error;

    fn try_from(value: &'a [u8]) -> std::result::Result<Self, Self::Error> {
        ensure!(value.len() >= AmsHeaderSlice::SIZE, "invalid AmsHeaderSlice");
        Ok(AmsHeaderSlice { data: value })
    }
}

impl<'a> Display for AmsHeaderSlice<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "target_net_id={}, ", self.target_net_id())?;
        write!(f, "target_port={}, ", self.target_port())?;
        write!(f, "source_net_id={}, ", self.source_net_id())?;
        write!(f, "source_port={}, ", self.source_port())?;
        write!(f, "command_id={}, ", self.command_id())?;
        write!(f, "state_flags={}, ", self.state_flags())?;
        write!(f, "length={}, ", self.length())?;
        write!(f, "error_code={}, ", self.error_code())?;
        write!(f, "invoke_id={}", self.invoke_id())
    }
}

pub fn parse_ams_packet_slice(data: &[u8]) -> Result<(AmsTcpHeaderSlice, AmsHeaderSlice, &[u8])> {
    let ams_tcp_header_slice = AmsTcpHeaderSlice::try_from(data[..AmsTcpHeaderSlice::SIZE].as_ref())?;
    let ams_header_slice = AmsHeaderSlice::try_from(
        data[AmsTcpHeaderSlice::SIZE..AmsTcpHeaderSlice::SIZE + AmsHeaderSlice::SIZE].as_ref(),
    )?;
    let content = &data[AmsTcpHeaderSlice::SIZE + AmsHeaderSlice::SIZE..];
    Ok((ams_tcp_header_slice, ams_header_slice, content))
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Debug mode
    #[arg(short, long, default_value_t = false)]
    pub debug: bool,

    /// Maximum AMS packet size
    #[arg(short, long, default_value_t = 65536)]
    pub packet_size: usize,

    /// Local listen address, e.g. 127.0.0.1:851
    pub listen_addr: SocketAddr,

    /// ADS backend tcp address, e.g. 172.18.0.10:851
    pub backend_addr: SocketAddr,
}

async fn read_ams_packet<T>(socket_addr: SocketAddr, reader: &mut T, buf: &mut [u8]) -> Result<usize>
where
    T: AsyncRead + Unpin,
{
    let mut size;
    let mut to_read;

    // check buffer
    ensure!(
        buf.len() >= AmsTcpHeaderSlice::SIZE + AmsHeaderSlice::SIZE,
        "invalid buffer size"
    );

    // read ams tcp header
    to_read = AmsTcpHeaderSlice::SIZE;
    reader.read_exact(&mut buf[..to_read]).await?;
    let ams_tcp_header = AmsTcpHeaderSlice::try_from(&buf[..to_read])?;
    log::debug!("socket {} AmsTcpHeader: {}", socket_addr, ams_tcp_header);
    size = to_read;

    // check buffer again
    ensure!(
        buf.len() >= AMS_TCP_HEADER_SIZE + ams_tcp_header.length() as usize,
        "invalid buffer size"
    );

    // read ams packet content
    to_read = size + ams_tcp_header.length() as usize;
    reader.read_exact(&mut buf[size..to_read]).await?;
    let ams_header = AmsHeaderSlice::try_from(&buf[size..])?;
    log::debug!("socket {} AmsHeader: {}", socket_addr, ams_header);
    size = to_read;

    Ok(size)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // init logging
    let log_level = if args.debug { "debug" } else { "info" };
    env_logger::init_from_env(Env::default().default_filter_or(log_level));

    // buffer size per connection
    let buffer_size = args.packet_size;
    let backend_addr = args.backend_addr;

    // global forward tables
    let forward_table = Arc::new(RwLock::new(HashMap::new()));

    // connect backend
    log::info!("connecting ads {}...", backend_addr);
    let ads_client = TcpStream::connect(backend_addr).await?;
    let (mut ads_read, ads_write) = ads_client.into_split();
    let ads_write = Arc::new(Mutex::new(ads_write));

    // listen for client
    log::info!("listening {}...", args.listen_addr);
    let listener = TcpListener::bind(args.listen_addr).await?;
    let forward_table_clone = forward_table.clone();
    tokio::spawn(async move {
        loop {
            // accept new client
            let (client, socket_addr) = listener.accept().await.unwrap();
            log::info!("new socket {}", socket_addr);

            // split reading and writing
            let (mut client_read, client_write) = client.into_split();
            let client_write = Arc::new(Mutex::new(client_write));

            // reading
            let forward_table = forward_table_clone.clone();
            let plc_write = ads_write.clone();
            tokio::spawn(async move {
                let mut buff = vec![0; buffer_size];

                loop {
                    // read ams packet
                    let size = match read_ams_packet(socket_addr, &mut client_read, buff.as_mut_slice()).await {
                        Ok(size) => size,
                        Err(e) => {
                            log::error!("read socket {} error: {}", socket_addr, e);
                            break;
                        }
                    };

                    // parse packet
                    let ams_header_slice = AmsHeaderSlice::try_from(&buff[AmsTcpHeaderSlice::SIZE..]).unwrap();
                    let ams_addr = AmsAddr(ams_header_slice.source_net_id(), ams_header_slice.source_port());

                    // update forward table
                    let client_socket = (socket_addr, client_write.clone());
                    if let Some(r) = forward_table.write().await.insert(ams_addr, client_socket) {
                        if r.0 != socket_addr {
                            log::info!("update {} socket {} -> {}", ams_addr, r.0, socket_addr);
                        }
                    }

                    // forward to plc
                    if let Err(e) = plc_write.lock().await.write_all(&buff[..size]).await {
                        log::error!("forward {}({}) to plc error: {}", ams_addr, socket_addr, e);
                        break;
                    }

                    log::debug!("forward {}({}) to plc, size {}", ams_addr, socket_addr, size);
                }

                // reading stop
                log::info!("reading {} stop", socket_addr);
            });
        }
    });

    // plc reading loop
    loop {
        let mut buff = vec![0; buffer_size];
        match read_ams_packet(backend_addr, &mut ads_read, &mut buff).await {
            Ok(size) => {
                // parse ams header
                let ams_header = AmsHeaderSlice::try_from(&buff[AmsTcpHeaderSlice::SIZE..]).unwrap();
                let ams_addr = AmsAddr(ams_header.source_net_id(), ams_header.source_port());

                // forward
                let mut has_error = false;
                if let Some((addr, write)) = forward_table.read().await.get(&ams_addr) {
                    let mut write = write.lock().await;
                    if let Err(e) = write.write_all(&buff[..size]).await {
                        log::error!("forward plc to {}({}) error: {}", ams_addr, addr, e);
                        has_error = true;
                    }
                } else {
                    log::warn!("ams {} not found", ams_addr)
                }

                // check error
                if has_error {
                    forward_table.write().await.remove(&ams_addr);
                }
            }
            Err(e) => {
                log::error!("read plc error: {}", e);
                break;
            }
        }
    }

    Ok(())
}
