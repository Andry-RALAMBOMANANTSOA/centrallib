use shared_memory::*;
use std::string;
use std::sync::atomic::{AtomicBool, Ordering};
use rmp_serde::{Serializer, Deserializer};
use serde::{Serialize, Deserialize};
use std::fs::File;
use std::io::{BufReader, Error};
use centralstruct::*;
use rand::prelude::*;
use std::sync::mpsc;
use dashmap::{DashMap,DashSet};
use chrono::{NaiveDateTime, TimeZone, Utc,DateTime};
use nng::Message;


pub fn ocsm(segment: &str) -> Result<Shmem, ShmemError> {
    match ShmemConf::new().os_id(segment).open() {
        Ok(v) => Ok(v),
        Err(e) => {
            match ShmemConf::new().size(4096).os_id(segment).create() {
                Ok(v) => Ok(v),
                Err(e) => Err(e),
            }
        }
    }
}


pub fn smproc(my_shmem: &Shmem) -> (&AtomicBool, &AtomicBool, &[u8]) {
    // Get the buffer from shared memory
    let buf: &[u8] = unsafe { my_shmem.as_slice() };

    // Use the first byte as a flag. 1 means data is written, 0 means data is read
    let data_written_flag = unsafe { &*(buf.as_ptr() as *const AtomicBool) };
    let ok_flag = unsafe { &*(buf.as_ptr().offset(1) as *const AtomicBool) };
    let data_buf = &buf[2..];

    (data_written_flag, ok_flag, data_buf)
}

pub fn wait_data(data_written_flag: &AtomicBool) {
    while !data_written_flag.load(Ordering::Relaxed) {
        // Spin-wait until the writer has written data
    }
}

pub fn resetdata_sendok(data_written_flag: &AtomicBool, ok_flag: &AtomicBool) {
    data_written_flag.store(false, Ordering::Relaxed);
    ok_flag.store(true, Ordering::Relaxed);
}

pub fn data_isavailable(data_written_flag: &AtomicBool) {
    data_written_flag.store(true, Ordering::Relaxed);
}

pub fn waitok_resetok(ok_flag: &AtomicBool) {
    while !ok_flag.load(Ordering::Relaxed) {
        // Spin-wait
    }
    ok_flag.store(false, Ordering::Relaxed);
}

pub fn read_marketconf(path: &str) -> Result<MarketConf, Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let result = serde_json::from_reader(reader)?;
    Ok(result)
}

pub fn id_i64() -> i64 {
    let mut rng = rand::thread_rng();
    let id = rng.gen::<i64>();
    if id == i64::MIN { i64::MAX } else if id < 0 { -id } else { id }
}

pub fn lowest_ask(ask_mbp: &MBPData) -> Option<i32> {
    if let Some((lowest_price,_)) = ask_mbp.mbp.iter().next() {
        Some(*lowest_price)
    } else {
        None
    }
}

pub fn highest_bid(bid_mbp: &MBPData) -> Option<i32> {
    if let Some((highest_price, _)) = bid_mbp.mbp.iter().next_back() {
        Some(*highest_price)
    } else {
        None
    }
}
pub fn lowest_ask_quant(ask_mbp: &MBPData) -> Option<i32> {
    if let Some((_,askquantity)) = ask_mbp.mbp.iter().next() {
        Some(*askquantity)
    } else {
        None
    }
}

pub fn highest_bid_quant(bid_mbp: &MBPData) -> Option<i32> {
    if let Some((_, bidquantity)) = bid_mbp.mbp.iter().next_back() {
        Some(*bidquantity)
    } else {
        None
    }
}

pub fn trade_struct_limit(arrival: &LimitOrder, trader_order: &TraderOrderStruct, trade_identifier: i64,taker_orderid:i64,time:i64,quantity:i32) -> TradeStruct {

    TradeStruct {
        market: arrival.market.clone(),
        broker_identifier_taker: arrival.broker_identifier.clone(),
        broker_identifier_maker: trader_order.broker_identifier.clone(),
        unix_time:time,
        trade_identifier,
        trader_identifier_taker: arrival.trader_identifier,
        order_identifier_taker: taker_orderid,
        trader_identifier_maker: trader_order.trader_identifier,
        order_identifier_maker: trader_order.order_identifier,
        taker_type:"limit".to_string(),
        expiration_taker: arrival.expiration.clone(),
        expiration_maker: trader_order.expiration.clone(),
        order_quantity: quantity,
        order_side: arrival.order_side.clone(),
        price: trader_order.price,
    }
}
pub fn trade_struct_market(arrival: &MarketOrder, trader_order: &TraderOrderStruct, trade_identifier: i64,taker_orderid:i64,time:i64,quantity:i32) -> TradeStruct {

    TradeStruct {
        market: arrival.market.clone(),
        broker_identifier_taker: arrival.broker_identifier.clone(),
        broker_identifier_maker: trader_order.broker_identifier.clone(),
        unix_time:time,
        trade_identifier,
        trader_identifier_taker: arrival.trader_identifier,
        order_identifier_taker: taker_orderid,
        trader_identifier_maker: trader_order.trader_identifier,
        order_identifier_maker: trader_order.order_identifier,
        taker_type:"market".to_string(),
        expiration_taker: arrival.expiration.clone(),
        expiration_maker: trader_order.expiration.clone(),
        order_quantity: quantity,
        order_side: arrival.order_side.clone(),
        price: trader_order.price,
    }
}

pub fn taker_info_limit(arrival: &LimitOrder, config: &MarketConf, traded_quantity: i32, price: i32, unix_time: i64) -> PostTraderInf {
    let mut cumas1:i32 =0;
    let mut cumas2:i32 =0;
    if arrival.order_side == "buy"{
        cumas1 =  traded_quantity; // base
        cumas2 =  - traded_quantity * price; // quote
    }
    else if arrival.order_side =="sell"{
        cumas1 =  - traded_quantity;
        cumas2 =  traded_quantity * price;
    }
    PostTraderInf {
        unix_time,
        market: config.market_name.clone(),
        broker_identifier: arrival.broker_identifier.clone(),
        trader_identifier: arrival.trader_identifier,
        asset1: config.asset1.clone(), //asset 1 is base
        asset2: config.asset2.clone(),// asset 2 is quote
        trader_calcbalance_asset1: cumas1, 
        trader_calcbalance_asset2:cumas2, 
    }
}
pub fn taker_info_market(arrival: &MarketOrder, config: &MarketConf, traded_quantity: i32, price: i32, unix_time: i64) -> PostTraderInf {
    let mut cumas1:i32 =0;
    let mut cumas2:i32 =0;
    if arrival.order_side == "buy"{
        cumas1 =  traded_quantity; // base
        cumas2 =  - traded_quantity * price; // quote
    }
    else if arrival.order_side =="sell"{
        cumas1 =  - traded_quantity;
        cumas2 =  traded_quantity * price;
    }
    PostTraderInf {
        unix_time,
        market: config.market_name.clone(),
        broker_identifier: arrival.broker_identifier.clone(),
        trader_identifier: arrival.trader_identifier,
        asset1: config.asset1.clone(), //asset 1 is base
        asset2: config.asset2.clone(),// asset 2 is quote
        trader_calcbalance_asset1: cumas1, 
        trader_calcbalance_asset2:cumas2, 
    }
}

pub fn maker_info(trader_order_struct: &TraderOrderStruct, config: &MarketConf, traded_quantity: i32, price: i32,unix_time: i64) -> PostTraderInf {
    let mut cumas1:i32 =0;
    let mut cumas2:i32 =0;
    if trader_order_struct.order_side == "buy"{
        cumas1 =  traded_quantity ; // base
        cumas2 =  - traded_quantity * price; // quote
    }
    else if trader_order_struct.order_side =="sell"{
        cumas1 =  - traded_quantity ;
        cumas2 =  traded_quantity * price;
    }
    PostTraderInf {
        unix_time,
        market: config.market_name.clone(),
        broker_identifier: trader_order_struct.broker_identifier.clone(),
        trader_identifier: trader_order_struct.trader_identifier,
        asset1: config.asset1.clone(), //asset 1 is base
        asset2: config.asset2.clone(), // asset 2 is quote
        trader_calcbalance_asset1: cumas1,
        trader_calcbalance_asset2: cumas2,
    }
}

pub fn time_sale(config: &MarketConf, unix_time: i64, order_quantity: i32, order_side: String, price: i32) -> TimeSale {
    TimeSale {
        market: config.market_name.clone(),
        exchange: config.exchange.clone(),
        unix_time,
        order_quantity,
        order_side: order_side.to_string(),
        price,
    }
}

pub fn tp_last(unix_time:i64,price: i32) -> Last {
    Last { unix_time,price }
}

pub fn struct_nbbo(unix_time: i64, ask_mbp: &MBPData, bid_mbp: &MBPData) -> NBBO {
    let ask_price = lowest_ask(ask_mbp);
    let bid_price = highest_bid(bid_mbp);
    let ask_size = lowest_ask_quant(ask_mbp); 
    let bid_size = highest_bid_quant(bid_mbp); 

    NBBO {
        unix_time,
        ask_price,
        bid_price,
        ask_size,
        bid_size,
    }
}

pub fn volume_struct(time:i64,quantity:i32,side:&String,price:i32)-> Volume {
    let sign_quant = match side.as_str() {
        "buy" => quantity,
        "sell" => -quantity,
        _ => panic!("Invalid side: {}", side),  // or some other default value or action
    };
    Volume {
        unix_time: time,
        volume: sign_quant,
        value:sign_quant*price,
        price,
    }
}

pub fn modify_struct (config: &MarketConf,trader_order_struct: &TraderOrderStruct,unix_time: i64,old_quant:i32,new_quant:i32 ) -> ModifiedOrderStruct {

    ModifiedOrderStruct {
    market: config.market_name.clone(),
    broker_identifier: trader_order_struct.broker_identifier.clone(),
    unix_time,
    trader_identifier: trader_order_struct.trader_identifier,
    order_identifier: trader_order_struct.order_identifier,
    older_order_quantity: old_quant,
    new_order_quantity: new_quant,
    order_side: trader_order_struct.order_side.clone(),
    expiration:trader_order_struct.expiration.clone(),
    price: trader_order_struct.price,
    }
}

pub fn modify_stop_struct (config: &MarketConf,trader_stop_order_struct: &TraderStopOrderStruct,unix_time: i64,old_quant:i32,new_quant:i32 ) -> ModifiedStopOrderStruct {

    ModifiedStopOrderStruct {
    market: config.market_name.clone(),
    broker_identifier: trader_stop_order_struct.broker_identifier.clone(),
    unix_time,
    trader_identifier: trader_stop_order_struct.trader_identifier,
    order_identifier: trader_stop_order_struct.order_identifier,
    older_order_quantity: old_quant,
    new_order_quantity: new_quant,
    order_side: trader_stop_order_struct.order_side.clone(),
    expiration:trader_stop_order_struct.expiration.clone(),
    trigger_price: trader_stop_order_struct.trigger_price,
    }
}

pub fn modify_stop_limit_struct (config: &MarketConf,trader_stop_limit_order_struct: &TraderStopLimitOrderStruct,unix_time: i64,old_quant:i32,new_quant:i32 ) -> ModifiedStopLimitOrderStruct {

    ModifiedStopLimitOrderStruct {
    market: config.market_name.clone(),
    broker_identifier: trader_stop_limit_order_struct.broker_identifier.clone(),
    unix_time,
    trader_identifier: trader_stop_limit_order_struct.trader_identifier,
    order_identifier: trader_stop_limit_order_struct.order_identifier,
    older_order_quantity: old_quant,
    new_order_quantity: new_quant,
    order_side: trader_stop_limit_order_struct.order_side.clone(),
    expiration:trader_stop_limit_order_struct.expiration.clone(),
    trigger_price: trader_stop_limit_order_struct.trigger_price,
    price: trader_stop_limit_order_struct.price,
    
    }
}
pub fn delete_struct (trader_order_struct: &TraderOrderStruct,unix_time: i64)-> DeletedOrderStruct {

    DeletedOrderStruct {
     market: trader_order_struct.market.clone(),
    broker_identifier: trader_order_struct.broker_identifier.clone(),
     unix_time,
     trader_identifier: trader_order_struct.trader_identifier,
     order_identifier:trader_order_struct.order_identifier,
     order_quantity: trader_order_struct.order_quantity,
     order_side: trader_order_struct.order_side.clone(),
     expiration:trader_order_struct.expiration.clone(),
     price: trader_order_struct.price,
    }

}
pub fn delete_stop_struct (trader_stop_order_struct: &TraderStopOrderStruct,unix_time: i64)-> DeletedStopOrderStruct {

    DeletedStopOrderStruct {
     market: trader_stop_order_struct.market.clone(),
    broker_identifier: trader_stop_order_struct.broker_identifier.clone(),
     unix_time,
     trader_identifier: trader_stop_order_struct.trader_identifier,
     order_identifier:trader_stop_order_struct.order_identifier,
     order_quantity: trader_stop_order_struct.order_quantity,
     order_side: trader_stop_order_struct.order_side.clone(),
     expiration:trader_stop_order_struct.expiration.clone(),
     trigger_price: trader_stop_order_struct.trigger_price,
    }
}
pub fn delete_stop_limit_struct (trader_stop_limit_order_struct: &TraderStopLimitOrderStruct,unix_time: i64)-> DeletedStopLimitOrderStruct {

        DeletedStopLimitOrderStruct {
         market: trader_stop_limit_order_struct.market.clone(),
        broker_identifier:trader_stop_limit_order_struct.broker_identifier.clone(),
         unix_time,
         trader_identifier: trader_stop_limit_order_struct.trader_identifier,
         order_identifier:trader_stop_limit_order_struct.order_identifier,
         order_quantity: trader_stop_limit_order_struct.order_quantity,
         order_side: trader_stop_limit_order_struct.order_side.clone(),
         expiration:trader_stop_limit_order_struct.expiration.clone(),
         trigger_price: trader_stop_limit_order_struct.trigger_price,
         price: trader_stop_limit_order_struct.price,
        }
    
    }


pub fn mbp_event (unix_time:i64,side:String,event_value:i32,event_price:i32,calc:i32,tx_market: &mpsc::Sender<Structs>,)  {
    
    let calculated_value = calc*event_value;
    let event = MBPEvents {
        unix_time,
        side:side.to_string(),
        event_value: calculated_value,
        event_price,
    };
    let event_message = Structs::MBPEvents(event);
    tx_market.send(event_message).unwrap();

}

pub fn ex_stop(
    stop_struct: &DashMap<i64, TraderStopOrderStruct>,
    stop_map: &mut MAPStopData,
    last_dyn: &Last,
    tx: &mpsc::Sender<Message>,//tx internal
    tx_broker: &mpsc::Sender<Structs>,//tx broker
) {
    let mut exist = false;
    // Check if there are any order ids at the last_dyn price
    if let Some(order_ids) = stop_map.map.get(&last_dyn.price) {
        // Iterate over each order id
        exist = true;
        for &order_id in order_ids {
            // Get the TraderStopOrderStruct associated with the order id
            if let Some((_,order_struct)) = stop_struct.remove(&order_id) {
                // Create a MarketOrder from the TraderStopOrderStruct
                let market_order = MarketOrder {
                    market: order_struct.market.clone(),
                    broker_identifier: order_struct.broker_identifier.clone(),
                    trader_identifier: order_struct.trader_identifier,
                    order_identifier: Some(order_struct.order_identifier),
                    order_quantity: order_struct.order_quantity,
                    order_side: order_struct.order_side.clone(),
                    expiration: order_struct.expiration.clone(),
                };
                let order_message = Structs::MarketOrder(market_order);
                let mut buf_order = Vec::new();
                order_message.serialize(&mut Serializer::new(&mut buf_order)).unwrap();
                let msg = Message::from(buf_order.as_slice());
                tx.send(msg).unwrap();

                let string_m = format! ("{} {}, {} {} {} Stop order id {} entered to market", Utc::now().timestamp_micros(),order_struct.market,order_struct.order_quantity,order_struct.order_side,order_struct.expiration,order_struct.order_identifier );
                let message = Messaging {
                    unix_time :  Utc::now().timestamp_micros(),
                    market : order_struct.market.clone(),
                    broker_identifier : order_struct.broker_identifier.clone(),
                    trader_identifier : order_struct.trader_identifier,
                    message : string_m,
                };
                let m_message = Structs::Messaging(message);
                tx_broker.send(m_message).unwrap();

            }
        }
        
    }
    if exist {
        stop_map.map.remove(&last_dyn.price);
        exist = false;
    }
}

pub fn ex_stop_limit(
    stop_limit_struct: &DashMap<i64, TraderStopLimitOrderStruct>,
    stop_limit_map: &mut MAPStopLimitData,
    last_dyn: &Last,
    tx: &mpsc::Sender<Message>,
    tx_broker: &mpsc::Sender<Structs>,//tx broker
) {
    let mut exist = false;
    // Check if there are any order ids at the last_dyn price
    if let Some(order_ids) = stop_limit_map.map.get(&last_dyn.price) {
        // Iterate over each order id
        exist = true;
        for &order_id in order_ids {
            // Get the TraderStopOrderStruct associated with the order id
            if let Some((_,order_struct)) = stop_limit_struct.remove(&order_id) {
                // Create a MarketOrder from the TraderStopOrderStruct
                let limit_order = LimitOrder {
                    market: order_struct.market.clone(),
                    broker_identifier: order_struct.broker_identifier.clone(),
                    trader_identifier: order_struct.trader_identifier,
                    order_identifier: Some(order_struct.order_identifier),
                    order_quantity: order_struct.order_quantity,
                    order_side: order_struct.order_side.clone(),
                    expiration: order_struct.expiration.clone(),
                    price: order_struct.price,
                };
                let order_message = Structs::LimitOrder(limit_order);
                 let mut buf_order = Vec::new();
                order_message.serialize(&mut Serializer::new(&mut buf_order)).unwrap();
                let msg = Message::from(buf_order.as_slice());
                tx.send(msg).unwrap();

                let string_m = format! ("{} {}, {} {} at {} price level {} Stop limit order id {} entered to market", Utc::now().timestamp_micros(),order_struct.market,order_struct.order_quantity,order_struct.order_side,order_struct.price,order_struct.expiration,order_struct.order_identifier );
                let message = Messaging {
                    unix_time :  Utc::now().timestamp_micros(),
                    market : order_struct.market.clone(),
                    broker_identifier : order_struct.broker_identifier.clone(),
                    trader_identifier : order_struct.trader_identifier,
                    message : string_m,
                };
                let m_message = Structs::Messaging(message);
                tx_broker.send(m_message).unwrap();
            }
        }
        
    }
    if exist {
        stop_limit_map.map.remove(&last_dyn.price);
        exist = false;
    }
}