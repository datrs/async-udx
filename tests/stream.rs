use std::{io, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use udx::{UdxSocket, UdxStream};

#[tokio::test]
async fn stream_read_write() -> io::Result<()> {
    eprintln!("ok go");
    run().await?;
    // drop(streama);
    // drop(streamb);
    // drop(socka);
    // drop(sockb);
    eprintln!("wait");
    tokio::time::sleep(Duration::from_secs(1)).await;
    eprintln!("done");
    Ok(())
}

async fn run() -> io::Result<()> {
    let ((socka, _sockb), (mut streama, mut streamb)) = create_pair().await?;
    assert_eq!(socka.local_addr().unwrap(), streamb.remote_addr());

    let msg = vec![1, 2, 3];
    streama.write_all(&msg).await?;
    let mut read = vec![0u8; 3];
    streamb.read_exact(&mut read).await?;
    assert_eq!(msg, read);
    eprintln!("now drop");
    Ok(())
}

#[tokio::test]
async fn stream_close() -> io::Result<()> {
    let ((socka, _sockb), (mut streama, mut streamb)) = create_pair().await?;
    assert_eq!(socka.local_addr().unwrap(), streamb.remote_addr());

    // write a message.
    let msg = vec![1, 2, 3];
    streama.write_all(&msg).await?;
    assert_eq!(streama.stats().inflight_packets, 1, "inflight 1 after send");

    // close the stream
    let close = streama.close();
    assert_eq!(
        streama.stats().inflight_packets,
        1,
        "inflight still 1 after send"
    );

    // wait until closing is complete == all packages flushed
    close.await;
    assert_eq!(
        streama.stats().inflight_packets,
        0,
        "inflight 0 after close await"
    );

    // ensure reading on other end still works
    let mut read = vec![0u8; 3];
    let res = streamb.read_exact(&mut read).await;
    let _res = res?;
    assert_eq!(msg, read, "read ok");

    // try to write on closed stream
    let res = streama.write_all(&msg).await;
    assert_eq!(
        res.err().unwrap().kind(),
        io::ErrorKind::ConnectionReset,
        "stream closed"
    );
    // try to read on closed stream
    let res = streama.read(&mut read).await;
    assert_eq!(
        res.err().unwrap().kind(),
        io::ErrorKind::ConnectionReset,
        "stream closed"
    );

    Ok(())
}

async fn create_pair() -> io::Result<((UdxSocket, UdxSocket), (UdxStream, UdxStream))> {
    let socka = UdxSocket::bind("127.0.0.1:0")?;
    let sockb = UdxSocket::bind("127.0.0.1:0")?;
    let addra = socka.local_addr()?;
    let addrb = sockb.local_addr()?;
    let streama = socka.connect(addrb, 1, 2)?;
    let streamb = sockb.connect(addra, 2, 1)?;
    Ok(((socka, sockb), (streama, streamb)))
}

#[tokio::test]
async fn test_sockets() -> io::Result<()> {
    let a = UdxSocket::bind("127.0.0.1:0")?;
    let b = UdxSocket::bind("127.0.0.1:0")?;

    a.send(b.local_addr()?, b"boo");
    let (_sender, msg) = b.recv().await?;
    assert_eq!(msg, b"boo");
    Ok(())
}

#[tokio::test]
async fn test_stream_msg_replayed_on_connect() -> io::Result<()> {
    let aid = 66;
    let bid = 77;
    let a = UdxSocket::bind("127.0.0.1:0")?;
    let b = UdxSocket::bind("127.0.0.1:0")?;

    let mut bstr = b.connect(a.local_addr()?, bid, aid)?;
    bstr.write_all(b"AAA").await?;
    // b"AAA" seen as socket msg bc other stream doesn't exist yet
    let (_, _x) = a.recv().await?;

    let mut astr = a.connect(b.local_addr()?, aid, bid)?;

    bstr.write_all(b"BBB").await?;
    let mut buf = vec![];
    astr.read_buf(&mut buf).await?;
    assert_eq!(&buf, b"AAABBB");
    Ok(())
}

#[tokio::test]
async fn test_halfopen_streams() -> io::Result<()> {
    let aid = 66;
    let bid = 77;
    let a_socket = UdxSocket::bind("127.0.0.1:0")?;
    let b_socket = UdxSocket::bind("127.0.0.1:0")?;

    // `b` creates a stream
    let mut b_stream = b_socket.connect(a_socket.local_addr()?, bid, aid)?;
    // `b_stream` sends a stream message to `a`'s address with `aid`.
    b_stream.write_all(b"AAA").await?;

    let (_, _x) = a_socket.recv().await?;
    // `a` receives socket message, despite `b` sending stream message, because `a` does not yet have a stream with id = `aid`
    assert!(_x != b"AAA");

    let a_half_str = a_socket.create_stream(aid)?;

    b_stream.write_all(b"BBB").await?;

    // b_stream message not seen as socket message to a bc a_stream is half open
    assert!(
        tokio::time::timeout(Duration::from_millis(100), a_socket.recv())
            .await
            .is_err()
    );

    let mut a_stream = a_half_str.connect(b_socket.local_addr()?, bid)?;
    b_stream.write_all(b"CCC").await?;
    // b_stream message not seen as socket message to a bc a_stream is open
    assert!(
        tokio::time::timeout(Duration::from_millis(100), a_socket.recv())
            .await
            .is_err()
    );
    let mut buf = vec![];
    a_stream.read_buf(&mut buf).await?;
    assert_eq!(&buf, b"AAABBBCCC");
    Ok(())
}
