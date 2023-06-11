use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{ensure, Result};
use byteorder::{LittleEndian, ReadBytesExt};
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

impl FromStr for AmsNetId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let parts: Vec<u8> = s.split('.').map(|x| x.parse().unwrap()).collect();
        ensure!(parts.len() == 6, "invalid AmsNetId");
        let mut ams_net_id = AmsNetId::default();
        ams_net_id.0[..6].copy_from_slice(&parts[..6]);
        Ok(ams_net_id)
    }
}

impl From<AmsNetId> for ads::AmsNetId {
    fn from(value: AmsNetId) -> Self {
        ads::AmsNetId::from_slice(&value.0).unwrap()
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
        ReadBytesExt::read_u16::<LittleEndian>(&mut rdr).unwrap()
    }

    pub fn length(&self) -> u32 {
        let mut rdr = Cursor::new(&self.data[2..6]);
        ReadBytesExt::read_u32::<LittleEndian>(&mut rdr).unwrap()
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
        ReadBytesExt::read_u16::<LittleEndian>(&mut rdr).unwrap()
    }

    fn read_u32(&self, offset: usize) -> u32 {
        let mut rdr = Cursor::new(&self.data[offset..offset + 4]);
        ReadBytesExt::read_u32::<LittleEndian>(&mut rdr).unwrap()
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
    pub buffer_size: usize,

    /// Extra ams_net_id routes, e.g. 10.10.10.10.10.10
    #[arg(short, long)]
    pub route: Vec<AmsNetId>,

    /// PLC username (optional, to add route)
    #[arg(short, long)]
    pub username: Option<String>,

    /// PLC password (optional, to add route)
    #[arg(short, long)]
    pub password: Option<String>,

    /// Proxy host name (optional, hostname or ip)
    #[arg(long)]
    pub host: Option<String>,

    /// Local listen address, e.g. 172.18.0.100:48898
    pub listen_addr: SocketAddr,

    /// ADS backend plc address, e.g. 172.18.0.10:48898
    pub plc_addr: SocketAddr,
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
    let buffer_size = args.buffer_size;
    let plc_addr = args.plc_addr;
    let username = args.username;
    let password = args.password;

    // global forward tables
    let forward_table = Arc::new(RwLock::new(HashMap::new()));

    // get plc info
    let plc_info = ads::udp::get_info((&plc_addr.ip().to_string(), ads::UDP_PORT))?;
    log::info!("plc net_id={}", plc_info.netid);
    log::info!("plc hostname={}", plc_info.hostname);
    log::info!("plc twincat_version={:?}", plc_info.twincat_version);
    log::info!("plc os_version={:?}", plc_info.os_version);
    log::info!("plc fingerprint={}", plc_info.fingerprint);

    // add extra route
    let plc_client = TcpStream::connect(plc_addr).await?;
    let local_addr = plc_client.local_addr().unwrap();
    let proxy_host = args.host.unwrap_or(local_addr.ip().to_string());
    log::debug!("add route, host {}", proxy_host);
    for ams_net_id in args.route {
        log::info!("add route {} to plc", ams_net_id);
        let route_name = format!("route-{}", ams_net_id);
        if let Err(e) = ads::udp::add_route(
            (&plc_addr.ip().to_string(), ads::UDP_PORT),
            ams_net_id.into(),
            &proxy_host,
            Some(&route_name),
            username.as_deref(),
            password.as_deref(),
            true,
        ) {
            log::error!("add route {} error: {}", ams_net_id, e);
            break;
        }
    }

    // connect plc backend
    log::info!("connecting ads {}...", plc_addr);
    let ads_client = TcpStream::connect(plc_addr).await?;
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
                    let socket_info = (socket_addr, client_write.clone());
                    if let Some(r) = forward_table.write().await.insert(ams_addr, socket_info) {
                        if r.0 != socket_addr {
                            log::info!("replace table entry {} socket {} -> {}", ams_addr, r.0, socket_addr);
                        }
                    } else {
                        log::info!("add table entry {} socket {}", ams_addr, socket_addr);
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
                forward_table.write().await.retain(|_, x| x.0 != socket_addr);
            });
        }
    });

    // plc reading loop
    loop {
        let mut buff = vec![0; buffer_size];
        match read_ams_packet(plc_addr, &mut ads_read, &mut buff).await {
            Ok(size) => {
                // parse ams header
                let ams_header = AmsHeaderSlice::try_from(&buff[AmsTcpHeaderSlice::SIZE..]).unwrap();
                let ams_addr = AmsAddr(ams_header.target_net_id(), ams_header.target_port());

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
