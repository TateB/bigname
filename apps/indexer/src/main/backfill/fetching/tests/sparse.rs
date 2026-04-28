use super::super::count_provider_logs;
use super::*;

#[test]
fn raw_only_sparse_backfill_materialization_slices_use_log_count_cap() -> Result<()> {
    let resolved_blocks = (10..=13)
        .map(|block_number| ProviderResolvedBlock {
            block_number,
            block_hash: format!("0x{block_number:064x}"),
        })
        .collect::<Vec<_>>();
    let mut logs_by_block = BTreeMap::new();
    logs_by_block.insert(10, provider_logs(10, 2));
    logs_by_block.insert(11, provider_logs(11, 1));
    logs_by_block.insert(12, provider_logs(12, 2));

    let materializations =
        raw_only_sparse_materialization_slices(&resolved_blocks, &logs_by_block, 3)?;

    assert_eq!(
        materializations
            .iter()
            .map(|materialization| materialization.range)
            .collect::<Vec<_>>(),
        vec![
            BackfillBlockRange {
                from_block: 10,
                to_block: 11,
            },
            BackfillBlockRange {
                from_block: 12,
                to_block: 13,
            },
        ]
    );
    assert_eq!(
        materializations
            .iter()
            .map(|materialization| count_provider_logs(&materialization.logs_by_block))
            .collect::<Vec<_>>(),
        vec![3, 2]
    );

    Ok(())
}

#[test]
fn raw_only_sparse_backfill_materialization_keeps_single_dense_block() -> Result<()> {
    let resolved_blocks = (10..=11)
        .map(|block_number| ProviderResolvedBlock {
            block_number,
            block_hash: format!("0x{block_number:064x}"),
        })
        .collect::<Vec<_>>();
    let mut logs_by_block = BTreeMap::new();
    logs_by_block.insert(10, provider_logs(10, 4));

    let materializations =
        raw_only_sparse_materialization_slices(&resolved_blocks, &logs_by_block, 3)?;

    assert_eq!(materializations.len(), 2);
    assert_eq!(
        materializations[0].range,
        BackfillBlockRange {
            from_block: 10,
            to_block: 10
        }
    );
    assert_eq!(count_provider_logs(&materializations[0].logs_by_block), 4);
    assert_eq!(
        materializations[1].range,
        BackfillBlockRange {
            from_block: 11,
            to_block: 11
        }
    );
    assert_eq!(count_provider_logs(&materializations[1].logs_by_block), 0);

    Ok(())
}

fn provider_logs(block_number: i64, count: usize) -> Vec<ProviderLog> {
    (0..count)
        .map(|index| ProviderLog {
            block_hash: format!("0x{block_number:064x}"),
            block_number,
            transaction_hash: format!("0x{index:064x}"),
            transaction_index: index as i64,
            log_index: index as i64,
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            topics: Vec::new(),
            data: "0x".to_owned(),
        })
        .collect()
}
