use redb::{Database, TableDefinition};

const TABLE: TableDefinition<u64, [u8]> = TableDefinition::new("TABLE");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let db = unsafe { Database::create("index.db", 4 << 40)? };

    for i in 0..1000000 {
        log::info!("{i}");
        let tx = db.begin_write()?;

        let mut table = tx.open_table(&TABLE)?;

        let value = vec![0; 256];

        for j in 0..10 {
            table.insert(&(i * 10 + j), &value)?;
        }

        tx.commit()?;
    }

    Ok(())
}
