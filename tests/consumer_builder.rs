mod testsupport;

use samsa::prelude::{ConsumerBuilder, Error, TcpConnection, TopicPartitions};

#[tokio::test]
async fn it_can_build_with_minimal_args() -> Result<(), Box<Error>> {
    let (skip, brokers) = testsupport::get_brokers()?;
    if skip {
        return Ok(());
    }
    let builder =
        ConsumerBuilder::<TcpConnection>::new(brokers, TopicPartitions::default(), None).await?;
    let _consumer = builder.build();
    Ok(())
}

#[tokio::test]
async fn it_can_build_with_ref_to_builder() -> Result<(), Box<Error>> {
    let (skip, brokers) = testsupport::get_brokers()?;
    if skip {
        return Ok(());
    }
    let builder =
        ConsumerBuilder::<TcpConnection>::new(brokers, TopicPartitions::default(), None).await?;
    let builder_ref = &builder;
    let _consumer = builder_ref.clone().build();
    Ok(())
}
