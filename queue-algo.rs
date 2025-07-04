// Level‑3 Order Book Pipeline (Rust async + lock‑free ring‑deque)
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::{task, time};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventKind {
    Add,
    Modify,
    Cancel,
    Execute,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderEvent {
    pub ts: u64,      // hardware timestamp (ns)
    pub kind: EventKind,
    pub order_id: u64,
    pub price: u64,   // price in integer ticks
    pub size: i32,    // +size for adds/mods, −filled for exec, 0 for cancel
    pub side: Side,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Side { Bid, Ask }

#[derive(Debug, Clone)]
pub struct OrderEntry {
    pub order_id: u64,
    pub size: i32,
    pub ts: u64,
}

type BookSide = DashMap<u64, VecDeque<OrderEntry>>;

#[derive(Debug)]
pub struct OrderBook {
    pub bids: BookSide,
    pub asks: BookSide,
}

impl OrderBook {
    pub fn new() -> Self {
        Self { bids: DashMap::new(), asks: DashMap::new() }
    }

    pub fn apply(&self, ev: &OrderEvent) {
        let side_map = match ev.side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        };
        match ev.kind {
            EventKind::Add => {
                let mut dq = side_map.entry(ev.price).or_insert_with(VecDeque::new);
                dq.push_back(OrderEntry { order_id: ev.order_id, size: ev.size, ts: ev.ts });
            }
            EventKind::Modify => {
                if let Some(mut dq) = side_map.get_mut(&ev.price) {
                    if let Some(entry) = dq.iter_mut().find(|o| o.order_id == ev.order_id) {
                        entry.size = ev.size;
                        entry.ts = ev.ts;
                    }
                }
            }
            EventKind::Cancel | EventKind::Execute => {
                if let Some(mut dq) = side_map.get_mut(&ev.price) {
                    if let Some(pos) = dq.iter().position(|o| o.order_id == ev.order_id) {
                        let mut entry = dq[pos].clone();
                        entry.size += ev.size;
                        if entry.size <= 0 { dq.remove(pos); }
                        else { dq[pos] = entry; }
                    }
                }
            }
        }
    }

    pub fn queue_ahead(&self, side: Side, price: u64, my_order: u64) -> i32 {
        let map = match side { Side::Bid => &self.bids, Side::Ask => &self.asks };
        if let Some(dq) = map.get(&price) {
            let mut sum = 0;
            for entry in dq.iter() {
                if entry.order_id == my_order { break };
                sum += entry.size.max(0);
            }
            sum
        } else { 0 }
    }
}

const RING_CAP: usize = 1 << 15; // 32k events

pub async fn start_pipeline() -> anyhow::Result<()> {
    let ring = Arc::new(ArrayQueue::<OrderEvent>::new(RING_CAP));
    let book = Arc::new(OrderBook::new());

    let rx_ring = ring.clone();
    task::spawn(async move {
        feed_reader(rx_ring).await;
    });

    let tx_ring = ring.clone();
    let book_clone = book.clone();
    task::spawn(async move {
        book_worker(tx_ring, book_clone).await;
    });

    strategy_loop(book).await;

    Ok(())
}

async fn feed_reader(ring: Arc<ArrayQueue<OrderEvent>>) {
    let mut ts: u64 = 0;
    loop {
        // TODO: pull bytes, parse ITCH/OUCH, etc.
        let ev = OrderEvent {
            ts,
            kind: EventKind::Add,
            order_id: ts % 1_000_000,
            price: 100_000,
            size: 100,
            side: Side::Bid,
        };
        if ring.push(ev).is_err() {
        }
        ts += 1_000; // +1 µs
    }
}

async fn book_worker(ring: Arc<ArrayQueue<OrderEvent>>, book: Arc<OrderBook>) {
    loop {
        if let Some(ev) = ring.pop() {
            book.apply(&ev);
        } else {
            task::yield_now().await;
        }
    }
}

async fn strategy_loop(book: Arc<OrderBook>) {
    let my_order_id = 42;
    let horizon_ms = 200.0;
    let p_min = 0.70;

    let lam_exec = 1.5_f64;
    let lam_cancel = 0.5_f64;

    let mut ticker = time::interval(Duration::from_millis(1));
    loop {
        ticker.tick().await;
        let q_ahead = book.queue_ahead(Side::Bid, 100_000, my_order_id) as f64;
        if q_ahead == 0.0 { continue; }
        let q_me = 100.0;
        let rate = (lam_exec + lam_cancel) * q_ahead / (q_ahead + q_me);
        let p_fill = 1.0 - (-rate * horizon_ms).exp();

        if p_fill < p_min {
            // --> send cancel / jump order via OUCH / FIX
            println!("🚨 consider jump: p_fill={:.2}% q_ahead={}", p_fill * 100.0, q_ahead);
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    start_pipeline().await
}
