use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use tracing::{info, warn};

const PRODUCT_NAMES: &[&str] = &[
    "Widget",
    "Gadget",
    "Doohickey",
    "Thingamajig",
    "Gizmo",
    "Contraption",
];

pub struct TrafficGenerator {
    connection_url: String,
    running: Arc<AtomicBool>,
}

impl TrafficGenerator {
    pub fn new(connection_url: String) -> Self {
        Self {
            connection_url,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub async fn run(&self) {
        info!("Traffic generator starting, connecting to PostgreSQL...");

        let (client, connection) =
            match tokio_postgres::connect(&self.connection_url, tokio_postgres::NoTls).await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!("Traffic generator failed to connect: {}", e);
                    return;
                }
            };

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                warn!("Traffic generator connection error: {}", e);
            }
        });

        info!("Traffic generator connected, generating ~200 events/sec");

        let mut rng = StdRng::from_entropy();

        while self.running.load(Ordering::Relaxed) {
            let roll: u32 = rng.gen_range(0..100);

            let result = if roll < 70 {
                insert_order(&client).await
            } else if roll < 95 {
                update_order(&client).await
            } else {
                delete_order(&client).await
            };

            if let Err(e) = result {
                warn!("Traffic generator DB error: {}", e);
            }

            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        info!("Traffic generator stopped");
    }
}

async fn insert_order(client: &tokio_postgres::Client) -> Result<(), tokio_postgres::Error> {
    let mut rng = StdRng::from_entropy();
    let customer_id: i32 = rng.gen_range(1..1000);
    let total: f64 = rng.gen_range(10.0..500.0);

    let sql = format!(
        "INSERT INTO orders (customer_id, total, status) VALUES ({}, {:.2}, 'pending') RETURNING id",
        customer_id, total
    );
    let row = client.query_one(&sql, &[]).await?;

    let order_id: i32 = row.get(0);
    let num_items: u32 = rng.gen_range(1..=3);

    for _ in 0..num_items {
        let product = PRODUCT_NAMES[rng.gen_range(0..PRODUCT_NAMES.len())];
        let quantity: i32 = rng.gen_range(1..10);
        let price: f64 = rng.gen_range(5.0..100.0);

        let item_sql = format!(
            "INSERT INTO order_items (order_id, product_name, quantity, price) VALUES ({}, '{}', {}, {:.2})",
            order_id, product, quantity, price
        );
        client.execute(&item_sql, &[])
            .await?;
    }

    Ok(())
}

async fn update_order(client: &tokio_postgres::Client) -> Result<(), tokio_postgres::Error> {
    let rows = client
        .query(
            "SELECT id, status FROM orders WHERE status != 'delivered' ORDER BY RANDOM() LIMIT 1",
            &[],
        )
        .await?;

    if let Some(row) = rows.first() {
        let id: i32 = row.get(0);
        let status: String = row.get(1);

        let next_status = match status.as_str() {
            "pending" => "processing",
            "processing" => "shipped",
            "shipped" => "delivered",
            _ => return Ok(()),
        };

        client
            .execute(
                "UPDATE orders SET status = $1, updated_at = NOW() WHERE id = $2",
                &[&next_status.to_string(), &id],
            )
            .await?;
    }

    Ok(())
}

async fn delete_order(client: &tokio_postgres::Client) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "DELETE FROM orders WHERE id = (SELECT id FROM orders WHERE status = 'pending' ORDER BY RANDOM() LIMIT 1)",
            &[],
        )
        .await?;

    Ok(())
}
