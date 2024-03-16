mod testsupport;

use samsa::prelude::redpanda::adminapi::AdminAPI;
use samsa::prelude::Error;

#[tokio::test]
async fn it_can_get_redpanda_adminapi_leader_id() -> Result<(), Box<Error>> {
    let (skip, urls) = testsupport::get_broker_urls()?;
    if skip {
        return Ok(());
    }
    let client = AdminAPI::builder().urls(urls).build()?;
    let leader_id = client.get_leader_id().await?;
    println!("Leader id is {}", leader_id);
    Ok(())
}
