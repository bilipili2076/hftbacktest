use std::sync::{Arc, Mutex};

use chrono::Utc;
use hashbrown::HashMap;
use tracing::{debug, info, warn};
use hftbacktest::types::{Order, OrderId, Status};

use crate::{
    connector::GetOrders,
    standx::{
        StandxError,
        msg::{rest::OrderDetail, stream::OrderUpdate},
    },
    utils::{RefSymbolOrderId, SymbolOrderId, generate_rand_string},
};

#[derive(Debug)]
struct OrderExt {
    symbol: String,
    order: Order,
    removed_by_ws: bool,
    removed_by_rest: bool,
}

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;
pub type ClientOrderId = String;

#[derive(Default, Debug)]
pub struct OrderManager {
    prefix: String,
    orders: HashMap<ClientOrderId, OrderExt>,
    order_id_map: HashMap<SymbolOrderId, ClientOrderId>,
}

impl OrderManager {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            orders: Default::default(),
            order_id_map: Default::default(),
        }
    }

    pub fn update_from_ws(&mut self, resp: &OrderUpdate) -> Result<Option<Order>, StandxError> {
        let cl_ord_id = match resp.cl_ord_id.as_ref() {
            Some(id) => id,
            None => {
                warn!(
                    symbol = %resp.symbol,
                    status = ?resp.status,
                    "standx order_manager: ws update missing cl_ord_id"
                );
                return Err(StandxError::OrderNotFound);
            }
        };

        if !cl_ord_id.starts_with(&self.prefix) {
            debug!(
                symbol = %resp.symbol,
                cl_ord_id = %cl_ord_id,
                prefix = %self.prefix,
                "standx order_manager: ws update prefix unmatched"
            );
            return Err(StandxError::PrefixUnmatched);
        }

        let order_ext = match self.orders.get_mut(cl_ord_id) {
            Some(v) => v,
            None => {
                warn!(
                    symbol = %resp.symbol,
                    cl_ord_id = %cl_ord_id,
                    known_orders = self.orders.len(),
                    "standx order_manager: ws update for unknown client_order_id"
                );
                return Err(StandxError::OrderNotFound);
            }
        };

        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;

        let resp_ts = resp.updated_at_ns().unwrap_or_default();
        if resp_ts < order_ext.order.exch_timestamp {
            debug!(
                cl_ord_id = %cl_ord_id,
                symbol = %order_ext.symbol,
                resp_ts = resp_ts,
                local_ts = order_ext.order.exch_timestamp,
                status = ?resp.status,
                "standx order_manager: ws update older than local exch_timestamp (ignored)"
            );
            return Ok(None);
        }

        if already_removed {
            debug!(
                cl_ord_id = %cl_ord_id,
                symbol = %order_ext.symbol,
                order_id = order_ext.order.order_id,
                removed_by_ws = order_ext.removed_by_ws,
                removed_by_rest = order_ext.removed_by_rest,
                status = ?resp.status,
                "standx order_manager: ws update received but order already marked removed"
            );
        }

        let qty = resp.qty.parse::<f64>().unwrap_or(order_ext.order.qty);
        let fill_qty = resp
            .fill_qty
            .parse::<f64>()
            .unwrap_or(order_ext.order.exec_qty);
        let price = resp
            .price
            .as_ref()
            .and_then(|p| p.parse::<f64>().ok())
            .unwrap_or(order_ext.order.price_tick as f64 * order_ext.order.tick_size);
        let fill_price = resp
            .fill_avg_price
            .as_ref()
            .and_then(|p| p.parse::<f64>().ok())
            .unwrap_or(order_ext.order.exec_price_tick as f64 * order_ext.order.tick_size);

        order_ext.order.status = resp.status;
        order_ext.order.req = Status::None;
        order_ext.order.qty = qty;
        order_ext.order.exec_qty = fill_qty;
        order_ext.order.exch_timestamp = resp_ts;

        order_ext.order.price_tick = (price / order_ext.order.tick_size).round() as i64;
        order_ext.order.exec_price_tick = (fill_price / order_ext.order.tick_size).round() as i64;

        info!(
            cl_ord_id = %cl_ord_id,
            symbol = %order_ext.symbol,
            order_id = order_ext.order.order_id,
            status = ?order_ext.order.status,
            qty = order_ext.order.qty,
            exec_qty = order_ext.order.exec_qty,
            price_tick = order_ext.order.price_tick,
            exec_price_tick = order_ext.order.exec_price_tick,
            removed_by_ws = order_ext.removed_by_ws,
            removed_by_rest = order_ext.removed_by_rest,
            "standx order_manager: ws applied"
        );

        let result = if already_removed {
            None
        } else {
            Some(order_ext.order.clone())
        };

        if order_ext.order.status != Status::New && order_ext.order.status != Status::PartiallyFilled {
            order_ext.removed_by_ws = true;

            warn!(
                cl_ord_id = %cl_ord_id,
                symbol = %order_ext.symbol,
                order_id = order_ext.order.order_id,
                status = ?order_ext.order.status,
                already_removed = already_removed,
                "standx order_manager: ws marks order terminal; removing from order_id_map"
            );

            if !already_removed {
                self.order_id_map.remove(&RefSymbolOrderId::new(
                    &order_ext.symbol,
                    order_ext.order.order_id,
                ));
            }

            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                self.orders.remove(cl_ord_id).unwrap();
            }
        }

        Ok(result)
    }


    pub fn update_submit_fail(&mut self, client_order_id: &ClientOrderId) -> Option<Order> {
        self.update_from_rest_fail(client_order_id, Some(Status::Expired))
    }

    pub fn update_cancel_fail(&mut self, client_order_id: &ClientOrderId) -> Option<Order> {
        self.update_from_rest_fail(client_order_id, None)
    }

    pub fn update_from_rest_fail(
        &mut self,
        client_order_id: &ClientOrderId,
        status: Option<Status>,
    ) -> Option<Order> {
        let order_ext = self.orders.get_mut(client_order_id)?;

        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;

        warn!(
            client_order_id = %client_order_id,
            symbol = %order_ext.symbol,
            order_id = order_ext.order.order_id,
            prev_status = ?order_ext.order.status,
            prev_req = ?order_ext.order.req,
            set_status = ?status,
            removed_by_ws = order_ext.removed_by_ws,
            removed_by_rest = order_ext.removed_by_rest,
            already_removed = already_removed,
            "standx order_manager: REST failed; applying fallback state"
        );

        if let Some(status) = status {
            order_ext.order.status = status;
        }
        order_ext.order.req = Status::None;

        let result = if already_removed {
            None
        } else {
            Some(order_ext.order.clone())
        };

        if order_ext.order.status != Status::New && order_ext.order.status != Status::PartiallyFilled {
            order_ext.removed_by_rest = true;

            warn!(
                client_order_id = %client_order_id,
                symbol = %order_ext.symbol,
                order_id = order_ext.order.order_id,
                status = ?order_ext.order.status,
                "standx order_manager: marking removed_by_rest and removing from order_id_map"
            );

            if !already_removed {
                self.order_id_map.remove(&RefSymbolOrderId::new(
                    &order_ext.symbol,
                    order_ext.order.order_id,
                ));
            }

            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                self.orders.remove(client_order_id).unwrap();
            }
        }

        result
    }


    pub fn update_from_rest(
        &mut self,
        client_order_id: &ClientOrderId,
        resp: &OrderDetail,
    ) -> Option<Order> {
        let order_ext = self.orders.get_mut(client_order_id)?;

        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;

        let resp_ts = resp.updated_at_ns().unwrap_or_default();
        if resp_ts < order_ext.order.exch_timestamp {
            debug!(
                client_order_id = %client_order_id,
                symbol = %order_ext.symbol,
                resp_ts = resp_ts,
                local_ts = order_ext.order.exch_timestamp,
                resp_status = ?resp.status,
                "standx order_manager: REST update older than local exch_timestamp (ignored)"
            );
            return None;
        }

        let qty = resp.qty.parse::<f64>().unwrap_or(order_ext.order.qty);
        let fill_qty = resp
            .fill_qty
            .parse::<f64>()
            .unwrap_or(order_ext.order.exec_qty);
        let price = resp
            .price
            .as_ref()
            .and_then(|p| p.parse::<f64>().ok())
            .unwrap_or(order_ext.order.price_tick as f64 * order_ext.order.tick_size);
        let fill_price = resp
            .fill_avg_price
            .as_ref()
            .and_then(|p| p.parse::<f64>().ok())
            .unwrap_or(order_ext.order.exec_price_tick as f64 * order_ext.order.tick_size);

        order_ext.order.status = resp.status;
        order_ext.order.req = Status::None;
        order_ext.order.qty = qty;
        order_ext.order.exec_qty = fill_qty;
        order_ext.order.exch_timestamp = resp_ts;

        order_ext.order.price_tick = (price / order_ext.order.tick_size).round() as i64;
        order_ext.order.exec_price_tick = (fill_price / order_ext.order.tick_size).round() as i64;

        info!(
            client_order_id = %client_order_id,
            symbol = %order_ext.symbol,
            order_id = order_ext.order.order_id,
            status = ?order_ext.order.status,
            qty = order_ext.order.qty,
            exec_qty = order_ext.order.exec_qty,
            price_tick = order_ext.order.price_tick,
            exec_price_tick = order_ext.order.exec_price_tick,
            removed_by_ws = order_ext.removed_by_ws,
            removed_by_rest = order_ext.removed_by_rest,
            already_removed = already_removed,
            "standx order_manager: REST applied"
        );

        let result = if already_removed {
            None
        } else {
            Some(order_ext.order.clone())
        };

        if order_ext.order.status != Status::New && order_ext.order.status != Status::PartiallyFilled {
            order_ext.removed_by_rest = true;

            // warn!(
            //     client_order_id = %client_order_id,
            //     symbol = %order_ext.symbol,
            //     order_id = order_ext.order.order_id,
            //     status = ?order_ext.order.status,
            //     "standx order_manager: marking removed_by_rest and removing from order_id_map"
            // );
            warn!(
                symbol = %order_ext.symbol,
                order_id = order_ext.order.order_id,
                cl_ord_id = %client_order_id,
                resp_status = ?resp.status,
                resp_id = ?resp.id,
                resp_cl = ?resp.cl_ord_id,
                removed_by_ws = order_ext.removed_by_ws,
                removed_by_rest = order_ext.removed_by_rest,
                "update_from_rest: status not active -> marking removed_by_rest and removing from order_id_map"
            );


            if !already_removed {
                self.order_id_map.remove(&RefSymbolOrderId::new(
                    &order_ext.symbol,
                    order_ext.order.order_id,
                ));
            }

            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                self.orders.remove(client_order_id).unwrap();
            }
        }

        result
    }


    pub fn prepare_client_order_id(&mut self, symbol: String, order: Order) -> Option<String> {
        let symbol_order_id = SymbolOrderId::new(symbol.clone(), order.order_id);
        if self.order_id_map.contains_key(&symbol_order_id) {
            warn!(
                symbol = %symbol,
                order_id = order.order_id,
                known_maps = self.order_id_map.len(),
                "standx order_manager: prepare_client_order_id hit existing order_id_map (duplicate order_id?)"
            );
            return None;
        }

        let client_order_id = format!("{}{}", self.prefix, generate_rand_string(16));
        if self.orders.contains_key(&client_order_id) {
            warn!(
                symbol = %symbol,
                order_id = order.order_id,
                client_order_id = %client_order_id,
                "standx order_manager: prepare_client_order_id generated an existing client_order_id (unexpected)"
            );
        }

        self.order_id_map
            .insert(symbol_order_id, client_order_id.clone());
        self.orders.insert(
            client_order_id.clone(),
            OrderExt {
                symbol: symbol.clone(),
                order,
                removed_by_ws: false,
                removed_by_rest: false,
            },
        );

        info!(
            symbol = %symbol,
            order_id = self.orders.get(&client_order_id).unwrap().order.order_id,
            client_order_id = %client_order_id,
            map_size = self.order_id_map.len(),
            orders_size = self.orders.len(),
            "standx order_manager: prepared new mapping"
        );

        Some(client_order_id)
    }


    pub fn get_client_order_id(&self, symbol: &str, order_id: OrderId) -> Option<String> {
        let key = RefSymbolOrderId::new(symbol, order_id);
        let out = self.order_id_map.get(&key).cloned();

        match &out {
            Some(id) => {
                debug!(
                    symbol = %symbol,
                    order_id = order_id,
                    client_order_id = %id,
                    "standx order_manager: get_client_order_id hit"
                );
            }
            None => {
                warn!(
                    symbol = %symbol,
                    order_id = order_id,
                    known_maps = self.order_id_map.len(),
                    "standx order_manager: get_client_order_id miss"
                );
            }
        }

        out
    }


    pub fn gc(&mut self) {
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        let stale_ts = now - 300_000_000_000;
        let stale_ids: Vec<(_, _)> = self
            .orders
            .iter()
            .filter(|&(_, wrapper)| {
                wrapper.order.status != Status::New
                    && wrapper.order.status != Status::PartiallyFilled
                    && wrapper.order.status != Status::Unsupported
                    && wrapper.order.exch_timestamp < stale_ts
            })
            .map(|(client_order_id, wrapper)| {
                (
                    client_order_id.clone(),
                    SymbolOrderId::new(wrapper.symbol.clone(), wrapper.order.order_id),
                )
            })
            .collect();
        for (client_order_id, order_id) in stale_ids.iter() {
            if self.order_id_map.contains_key(order_id) {
                self.order_id_map.remove(order_id).unwrap();
            }
            self.orders.remove(client_order_id);
        }
    }

    pub fn cancel_all_from_rest(&mut self, symbol: &str) -> Vec<Order> {
        let mut removed_orders = Vec::new();
        let mut removed_order_ids = Vec::new();
        for (client_order_id, order_ext) in &mut self.orders {
            if order_ext.symbol != symbol {
                continue;
            }
            let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;

            order_ext.removed_by_rest = true;
            order_ext.order.status = Status::Canceled;
            order_ext.order.exch_timestamp = Utc::now().timestamp_nanos_opt().unwrap();
            if !already_removed {
                self.order_id_map
                    .remove(&RefSymbolOrderId::new(symbol, order_ext.order.order_id));
                removed_orders.push(order_ext.order.clone());
            }

            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                removed_order_ids.push(client_order_id.clone());
            }
        }

        for order_id in removed_order_ids {
            self.orders.remove(&order_id).unwrap();
        }
        removed_orders
    }
}

impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        self.orders
            .iter()
            .filter(|(_, order)| {
                symbol.as_ref().map(|s| order.symbol == *s).unwrap_or(true) && order.order.active()
            })
            .map(|(_, order)| &order.order)
            .cloned()
            .collect()
    }
}
