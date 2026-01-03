use algo::gridtrading;
use hftbacktest::{
    live::{
        Instrument,
        LiveBot,
        LiveBotBuilder,
        LoggingRecorder,
        ipc::iceoryx::IceoryxUnifiedChannel,
    },
    prelude::{Bot, HashMapMarketDepth},
};

mod algo;

const ORDER_PREFIX: &str = "standx";

fn prepare_live() -> LiveBot<IceoryxUnifiedChannel, HashMapMarketDepth> {
    let mut hbt = LiveBotBuilder::new()
        .register(Instrument::new(
            "standx",
            "BTC-USD",
            0.01,
            0.0001,
            HashMapMarketDepth::new(0.01, 0.0001),
            0,
        ))
        .build()
        .unwrap();

    hbt
}

fn main() {
    tracing_subscriber::fmt::init();

    let mut hbt = prepare_live();

    let relative_half_spread = 0.0002;
    let relative_grid_interval = 0.0005;
    let grid_num = 3;
    let min_grid_step = 0.01; // tick size
    let skew = 0.0;
    let order_qty = 0.01;
    let max_position = 0.2;

    let mut recorder = LoggingRecorder::new();
    gridtrading(
        &mut hbt,
        &mut recorder,
        relative_half_spread,
        relative_grid_interval,
        grid_num,
        min_grid_step,
        skew,
        order_qty,
        max_position,
    )
    .unwrap();
    hbt.close().unwrap();
}
