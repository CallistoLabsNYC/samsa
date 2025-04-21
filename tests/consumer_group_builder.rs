mod testsupport;

use samsa::prelude::{BrokerAddress, ConsumerGroupBuilder, Error, TcpConnection, TopicPartitions, TopicPartitionsBuilder};

#[tokio::test]
async fn it_can_build_with_minimal_args() -> Result<(), Box<Error>> {
    let (skip, brokers) = testsupport::get_brokers()?;
    if skip {
        return Ok(());
    }
    let builder = ConsumerGroupBuilder::<TcpConnection>::new(
        brokers,
        "abc".to_string(),
        TopicPartitions::default(),
    )
    .await?;
    let _consumer = builder.build();
    Ok(())
}

#[tokio::test]
async fn it_can_build_with_ref_to_builder() -> Result<(), Box<Error>> {
    let (skip, brokers) = testsupport::get_brokers()?;
    if skip {
        return Ok(());
    }
    let builder = ConsumerGroupBuilder::<TcpConnection>::new(
        brokers,
        "abc".to_string(),
        TopicPartitions::default(),
    )
    .await?;
    let builder_ref = &builder;
    let _consumer = builder_ref.clone().build();
    Ok(())
}


#[tokio::test]
async fn it_sets_params_correctly() -> Result<(), Box<Error>> {
    let bootstrap_address = vec![BrokerAddress {
        host: "localhost".to_owned(),
        port: 9092
    }];

    let partitions = vec![0];
    let topic_name = "benchmark".to_string();
    let assignment = TopicPartitionsBuilder::new()
        .assign(topic_name, partitions)
        .build();

    let consumer = ConsumerGroupBuilder::<TcpConnection>::new(
        bootstrap_address,
        "ism".to_string(),
        assignment,
    ).await
        .expect("Could not create consumer.")
        .client_id("ism-1".parse().unwrap()) //SETTING MY CLIENT ID
        .max_wait_ms(1024)
        .min_bytes(1024)
        .max_bytes(1024)
        .max_partition_bytes(1024)
        .isolation_level(60).build()
        .await
        .expect("Could not create consumer.");

    assert_eq!(consumer.client_id,"ism-1");
    assert_eq!(consumer.fetch_params.max_wait_ms, 1024);
    assert_eq!(consumer.fetch_params.max_wait_ms, 1024);
    assert_eq!(consumer.fetch_params.min_bytes, 1024);
    assert_eq!(consumer.fetch_params.max_bytes, 1024);
    assert_eq!(consumer.fetch_params.max_partition_bytes, 1024);
    assert_eq!(consumer.fetch_params.isolation_level, 60);

    Ok(())
}