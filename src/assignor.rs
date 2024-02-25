use crate::{
    error::{Error, Result},
    protocol::{MemberAssignment, PartitionAssignment},
};

pub const ROUND_ROBIN_PROTOCOL: &str = "roundrobin";
#[allow(dead_code)]
pub const RANGE_PROTOCOL: &str = "range";
const DEFAULT_VERSION: i16 = 3;

pub fn assign<'a>(
    strategy: &str,
    assigned_topic_partitions: Vec<(&'a str, &Vec<i32>)>,
    number_of_consumers: usize,
) -> Result<Vec<MemberAssignment<'a>>> {
    match strategy {
        ROUND_ROBIN_PROTOCOL => Ok(round_robin(assigned_topic_partitions, number_of_consumers)),
        // RANGE_PROTOCOL => Ok(range(assigned_topic_partitions, number_of_consumers)),
        _ => Err(Error::AssignmentStrategyNotSupported(strategy.to_string())),
    }
}

/// The round robin assignor lays out all the available partitions and
/// all the available consumers. It then proceeds to do a round robin
/// assignment from partition to consumer. If the subscriptions of all
/// consumer instances are identical, then the partitions will be
/// uniformly distributed. (i.e., the partition ownership counts will
/// be within a delta of exactly one across all consumers.)
fn round_robin<'a>(
    mut assigned_topic_partitions: Vec<(&'a str, &Vec<i32>)>,
    number_of_consumers: usize,
) -> Vec<MemberAssignment<'a>> {
    let mut member_assignments = vec![
        MemberAssignment {
            version: DEFAULT_VERSION,
            partition_assignments: vec![],
            user_data: None
        };
        number_of_consumers
    ];

    assigned_topic_partitions.sort_by(|a, b| a.0.cmp(b.0));

    // initialize the assignments
    for (topic_name, _) in assigned_topic_partitions.clone() {
        tracing::info!("{}", topic_name);
        for member_assignment in member_assignments.iter_mut().take(number_of_consumers) {
            member_assignment
                .partition_assignments
                .push(PartitionAssignment {
                    topic_name,
                    partitions: vec![],
                });
        }
    }

    for (topic_count, (_, partitions)) in assigned_topic_partitions.iter().enumerate() {
        for (partition_count, partition) in partitions.iter().enumerate() {
            let member_index = ((topic_count + 1) + (partition_count + 1)) % number_of_consumers;
            member_assignments[member_index].partition_assignments[topic_count]
                .partitions
                .push(*partition);
        }
    }

    member_assignments
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::{assign, ROUND_ROBIN_PROTOCOL};

    #[test]
    fn test_roundrobin_assignor() {
        // For example, suppose there are two consumers C0 and C1, two topics t0 and t1, and each topic has 3 partitions, resulting in partitions t0p0, t0p1, t0p2, t1p0, t1p1, and t1p2.
        let number_of_consumers = 2;
        let topics = HashMap::from([
            (String::from("t0"), vec![0, 1, 2]),
            (String::from("t1"), vec![0, 1, 2]),
        ]);
        let assigned_topic_partitions: Vec<(&str, &Vec<i32>)> =
            topics.iter().map(|(a, b)| (a.as_ref(), b)).collect();
        let assignments = assign(
            ROUND_ROBIN_PROTOCOL,
            assigned_topic_partitions,
            number_of_consumers,
        )
        .unwrap();

        // C0: [t0p0, t0p2, t1p1]
        assert_eq!(assignments[0].partition_assignments[0].topic_name, "t0");
        assert_eq!(
            assignments[0].partition_assignments[0].partitions,
            vec![0, 2]
        );
        assert_eq!(assignments[0].partition_assignments[1].topic_name, "t1");
        assert_eq!(assignments[0].partition_assignments[1].partitions, vec![1]);
        // C1: [t0p1, t1p0, t1p2]
        assert_eq!(assignments[1].partition_assignments[0].topic_name, "t0");
        assert_eq!(assignments[1].partition_assignments[0].partitions, vec![1]);
        assert_eq!(assignments[1].partition_assignments[1].topic_name, "t1");
        assert_eq!(
            assignments[1].partition_assignments[1].partitions,
            vec![0, 2]
        );
    }
}
