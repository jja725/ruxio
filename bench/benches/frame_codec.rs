use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use ruxio_protocol::frame::{Frame, FrameReader, MessageType};
use ruxio_protocol::messages::ReadRangeRequest;

fn bench_frame_encode(c: &mut Criterion) {
    let req = ReadRangeRequest {
        uri: "gs://bucket/table/part-00000.parquet".into(),
        offset: 4096,
        length: 4 * 1024 * 1024,
    };
    let frame = Frame::new_json(MessageType::ReadRange, 1, &req);

    c.bench_function("frame_encode", |b| {
        b.iter(|| {
            frame.encode();
        });
    });
}

fn bench_frame_decode(c: &mut Criterion) {
    let req = ReadRangeRequest {
        uri: "gs://bucket/table/part-00000.parquet".into(),
        offset: 4096,
        length: 4 * 1024 * 1024,
    };
    let frame = Frame::new_json(MessageType::ReadRange, 1, &req);
    let encoded = frame.encode();

    c.bench_function("frame_decode", |b| {
        b.iter(|| {
            Frame::decode(&encoded).unwrap();
        });
    });
}

fn bench_frame_reader_throughput(c: &mut Criterion) {
    // Create 100 frames concatenated
    let mut all_bytes = Vec::new();
    for i in 0..100 {
        let frame = Frame::done(i);
        all_bytes.extend_from_slice(&frame.encode());
    }

    c.bench_function("frame_reader_100_frames", |b| {
        b.iter(|| {
            let mut reader = FrameReader::new();
            reader.feed(&all_bytes);
            let mut count = 0;
            while reader.next_frame().unwrap().is_some() {
                count += 1;
            }
            assert_eq!(count, 100);
        });
    });
}

criterion_group!(
    frame_codec_benches,
    bench_frame_encode,
    bench_frame_decode,
    bench_frame_reader_throughput
);
criterion_main!(frame_codec_benches);
