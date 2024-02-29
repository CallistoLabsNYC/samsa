mod testsupport;

use samsa::prelude::{ConsumerBuilder, Error, TopicPartitions};

#[tokio::test]
async fn it_can_build_with_minimal_args() -> Result<(), Box<Error>> {
    let (skip, brokers, _topic) = testsupport::get_brokers_and_topic()?;
    if skip {
        return Ok(());
    }
    let builder = ConsumerBuilder::new(brokers, TopicPartitions::default()).await?;
    let _consumer = builder.build();
    Ok(())
}

#[tokio::test]
async fn it_can_build_with_ref_to_builder() -> Result<(), Box<Error>> {
    let (skip, brokers, _topic) = testsupport::get_brokers_and_topic()?;
    if skip {
        return Ok(());
    }
    let builder = ConsumerBuilder::new(brokers, TopicPartitions::default()).await?;
    let builder_ref = &builder;
    let _consumer = builder_ref.clone().build();
    Ok(())
}
