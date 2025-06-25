use sqlx::PgPool;

pub async fn init_db() -> anyhow::Result<PgPool> {
    let pool = PgPool::connect("postgres://user:pass@localhost/paladin").await?;
    sqlx::migrate!().run(&pool).await?;
    Ok(pool)
}