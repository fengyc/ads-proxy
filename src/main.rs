use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{ensure, Result};
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::BytesMut;
use clap::Parser;
use env_logger::Env;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::{broadcast, mpsc, RwLock};

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

impl From<ads::AmsNetId> for AmsNetId {
    fn from(value: ads::AmsNetId) -> Self {
        AmsNetId(value.0)
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
    #[arg(short, long)]
    pub debug: bool,

    /// Buffer size (maximum size of a AMS packet)
    #[arg(short, long, default_value_t = 65536)]
    pub buffer_size: usize,

    /// Queue size (cached packets per connection)
    #[arg(short, long, default_value_t = 128)]
    pub queue_size: usize,

    /// Optional ams net id route, e.g. 10.10.10.10.1.1
    #[arg(short, long)]
    pub route_ams_net_id: Option<AmsNetId>,

    /// Optional PLC username (to add route)
    #[arg(short, long)]
    pub username: Option<String>,

    /// Optional PLC password (to add route)
    #[arg(short, long)]
    pub password: Option<String>,

    /// Optional Proxy hostname (hostname or ip address, detected from PLC connection)
    #[arg(long)]
    pub route_host: Option<String>,

    /// Proxy listen address
    #[arg(short, long, default_value = "127.0.0.1:48898")]
    pub listen_addr: SocketAddr,

    /// PLC address, e.g. 172.18.0.10:48898
    pub plc_addr: SocketAddr,
}

async fn read_ams_packet<T>(reader: &mut T, buf: &mut BytesMut, packet_size: usize) -> Result<usize>
where
    T: AsyncRead + Unpin,
{
    // check buffer
    buf.reserve(AmsTcpHeaderSlice::SIZE);
    unsafe { buf.set_len(AmsTcpHeaderSlice::SIZE) };

    // read ams tcp header
    reader.read_exact(&mut buf[..]).await?;
    let ams_tcp_header = AmsTcpHeaderSlice::try_from(&buf[..])?;
    let ams_length = ams_tcp_header.length() as usize;

    // check buffer again
    ensure!(ams_length <= packet_size - AmsTcpHeaderSlice::SIZE, "invalid packet");
    buf.reserve(ams_length);
    unsafe { buf.set_len(buf.len() + ams_length) }

    // read ams packet content
    reader.read_exact(&mut buf[AmsTcpHeaderSlice::SIZE..]).await?;

    Ok(buf.len())
}

type EventReceiver = broadcast::Receiver<()>;
type DataSender = mpsc::Sender<BytesMut>;
type DataReceiver = mpsc::Receiver<BytesMut>;

type Table = Arc<RwLock<HashMap<AmsAddr, (SocketAddr, DataSender)>>>;

async fn reading<T>(
    remote: SocketAddr,
    reader: T,
    stop_rx: EventReceiver,
    max_packet_size: usize,
    data_tx: DataSender,
    table: Table,
) -> Result<()>
where
    T: AsyncRead + Unpin,
{
    let mut reader = reader;
    let mut buffer = BytesMut::with_capacity(max_packet_size);
    let mut stop_rx = stop_rx;

    loop {
        // read packet until error or stopped
        let _n = select! {
            _ = stop_rx.recv() => break,
            r = read_ams_packet(&mut reader, &mut buffer, max_packet_size) => r?,
        };

        // parse result
        let ams_header = parse_ams_packet_slice(&buffer)?.1;
        log::debug!("ams packet: {}", ams_header);
        let source_ams_addr = AmsAddr(ams_header.source_net_id(), ams_header.source_port());
        let target_ams_addr = AmsAddr(ams_header.target_net_id(), ams_header.target_port());

        // update forward table
        if Some(true) != table.read().await.get(&source_ams_addr).map(|x| x.0 == remote) {
            log::info!("update forward table {} socket {}", source_ams_addr, remote);
            let mut table = table.write().await;
            table.insert(source_ams_addr, (remote, data_tx.clone()));
        }

        // forward data
        let packet = buffer.split();
        let forward_table = table.read().await;
        let target = forward_table.get(&target_ams_addr);
        let target = target.or_else(|| forward_table.get(&AmsAddr(target_ams_addr.0, 0)));
        if let Some((target_remote, target_sender)) = target {
            log::debug!("forward {} to socket {}", target_ams_addr, target_remote);
            target_sender.send(packet).await?;
        } else {
            log::debug!("can not handle {}", target_ams_addr)
        }
    }

    log::info!("reading socket {} stopped", remote);

    Ok(())
}

async fn writing<T>(socket_addr: SocketAddr, writer: T, stop_rx: EventReceiver, data_rx: DataReceiver) -> Result<()>
where
    T: AsyncWrite + Unpin,
{
    let mut writer = writer;
    let mut stop_rx = stop_rx;
    let mut data_rx = data_rx;

    loop {
        // read packet
        let packet = select! {
            _ = stop_rx.recv() => break,
            p = data_rx.recv() => match p {
                Some(p) => p,
                _ => break,
            },
        };
        if packet.is_empty() {
            break;
        }
        // write data
        select! {
            _ = stop_rx.recv() => break,
            r = writer.write_all(&packet) => r?,
        }
        log::debug!("write socket {} {} bytes", socket_addr, packet.len());
    }

    log::info!("writing socket {} stopped", socket_addr);

    Ok(())
}

async fn connect_plc(args: Arc<Args>, table: Table) -> Result<()> {
    // detect plc info
    let plc_info = ads::udp::get_info((&args.plc_addr.ip().to_string(), ads::UDP_PORT))?;
    log::info!("plc net_id={}", plc_info.netid);
    log::info!("plc hostname={}", plc_info.hostname);
    log::info!("plc twincat_version={:?}", plc_info.twincat_version);
    log::info!("plc os_version={:?}", plc_info.os_version);
    log::info!("plc fingerprint={}", plc_info.fingerprint);

    // connect plc backend
    log::info!("connecting plc {}...", args.plc_addr);
    let mut plc_client = TcpStream::connect(args.plc_addr).await?;
    let plc_addr = plc_client.peer_addr()?;
    let host_addr = plc_client.local_addr()?;

    // add extra route
    if let Some(ams_net_id) = args.route_ams_net_id {
        let route_host = args.route_host.clone().unwrap_or(host_addr.ip().to_string());
        log::info!("add route {} host {} to plc", ams_net_id, route_host);
        ads::udp::add_route(
            (&plc_addr.ip().to_string(), ads::UDP_PORT),
            ams_net_id.into(),
            route_host.as_str(),
            None,
            args.username.as_deref(),
            args.password.as_deref(),
            true,
        )?;

        // reconnect plc
        plc_client.shutdown().await?;
        plc_client = TcpStream::connect(args.plc_addr).await?;
    }

    // prepare queues
    let (read, write) = plc_client.into_split();
    let (data_tx, data_rx) = mpsc::channel(args.queue_size);
    let (stop_tx, stop_rx) = broadcast::channel(1);

    // update forward table with plc ams net id
    log::info!("updating forward table with plc {}", plc_info.netid);
    let plc_ams_net_id = plc_info.netid.into();
    let plc_ams_addr = AmsAddr(plc_ams_net_id, 0);
    table.write().await.insert(plc_ams_addr, (plc_addr, data_tx.clone()));

    // run until error or stopped
    let stop_rx1 = stop_rx.resubscribe();
    let reading = reading(plc_addr, read, stop_rx1, args.buffer_size, data_tx, table);
    let writing = writing(plc_addr, write, stop_rx, data_rx);

    if let Err(e) = select! {
        r = tokio::spawn(reading) => r,
        r = tokio::spawn(writing) => r,
    } {
        log::error!("plc error: {}", e);
    }
    stop_tx.send(())?;

    log::warn!("connect plc stopped");

    Ok(())
}

async fn accept_client(args: Arc<Args>, table: Table) -> Result<()> {
    let server: TcpListener = TcpListener::bind(args.listen_addr).await?;

    loop {
        let (client, remote) = server.accept().await?;
        log::info!("new socket {}", remote);

        let args = args.clone();
        let table = table.clone();
        let packet_size = args.buffer_size;
        let queue_size = args.queue_size;
        tokio::spawn(async move {
            let (client_read, client_write) = client.into_split();
            let (data_tx, data_rx) = mpsc::channel(queue_size);
            let (stop_tx, stop_rx) = broadcast::channel(2);

            let stop_rx1 = stop_rx.resubscribe();
            let data_tx1 = data_tx.clone();
            let writing = writing(remote, client_write, stop_rx1, data_rx);
            let reading = reading(remote, client_read, stop_rx, packet_size, data_tx1, table.clone());

            if let Err(e) = select! {
                r = tokio::spawn(reading) => r,
                w = tokio::spawn(writing) => w,
            } {
                log::error!("client {} error: {}", remote, e);
            }
            if let Err(e) = stop_tx.send(()) {
                log::error!("stop client {} error: {}", remote, e);
            }

            // clean table
            table.write().await.retain(|a, x| {
                if x.0 == remote {
                    log::info!("remove table entry {} socket {}", a, x.0);
                    false
                } else {
                    true
                }
            });
        });
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Arc::new(Args::parse());

    // init logging
    let log_level = if args.debug { "debug" } else { "info" };
    env_logger::init_from_env(Env::default().default_filter_or(log_level));

    // global forward tables and stop event
    let forward_table: Table = Arc::new(RwLock::new(HashMap::new()));

    let plc_task = connect_plc(args.clone(), forward_table.clone());
    let accept_task = accept_client(args.clone(), forward_table.clone());

    if let Err(e) = select! {
        r = tokio::spawn(plc_task) => r,
        r = tokio::spawn(accept_task) => r,
    } {
        log::error!("ads-proxy error: {}", e);
    }

    log::warn!("ads-proxy stopped");

    Ok(())
}
