#include <service/internal_service.h>
#include <exec/aggregator.h>
#include <testutil/desc_tbl_helper.h>
#include "exec/stream/stream_test.h"
#include "testutil/desc_tbl_helper.h"
#include <column/chunk.h>
#include <column/vectorized_fwd.h>

#include <memory>
#include <gtest/gtest.h>

namespace starrocks::stream {

class BenchAggregator : public StreamTestBase {
public:
    void SetUp() override {
        runtime_state = obj_pool.add(new RuntimeState(
                    /*fragment_instantce_id*/TUniqueId(),
                    /*query_options*/TQueryOptions(),
                    /*query_globals*/TQueryGlobals(),
                    /*exec_env*/nullptr));
        runtime_profile = runtime_state->runtime_profile();

        // sum(c2) group by c1
        group_by_infos = {0};

        auto slot_infos = std::vector<std::vector<SlotTypeInfo>>{
            // input slots
            {
                {"col1_decimal128", TYPE_DECIMAL128, true},
                {"col2_decimal128", TYPE_DECIMAL128, true},
            },
            // result slots
            {
                {"col1_decimal128", TYPE_DECIMAL128, true},
                {"sum_agg", TYPE_DECIMAL128, true},
            },
        };

        tbl = GenerateDescTbl(runtime_state, obj_pool, slot_infos);
        runtime_state->set_desc_tbl(tbl);

        auto agg_infos = std::vector<AggInfo>{
            {
             1, // slot_index
             "sum", // agg_name
             TYPE_DECIMAL128, // agg_intermediate_type
             TYPE_DECIMAL128, // agg_result_type
            },
        };
        aggregator = createAggregator(slot_infos, group_by_infos, agg_infos);
        DCHECK_IF_ERROR(aggregator->prepare(runtime_state, &obj_pool, runtime_profile));
        DCHECK_IF_ERROR(aggregator->open(runtime_state));

        input_chunks = genAllChunks(40000000);
        LOG(INFO) << "gjt debug agg input chunk: " << input_chunks.size();
        // for (size_t i = 0; i < input_chunks.size(); ++i) {
        //     for (size_t j = 0; j < input_chunks[i]->num_rows(); ++j) {
        //         LOG(INFO) << input_chunks[i]->debug_row(j);
        //     }
        // }
    }


    // mock table scan output.
    // schema: <col1 decimal128, col2 decimal128>
    ChunkPtr genOneChunk(size_t rows) {
        const int prec = 19;
        const int scale = 6;

        auto col1 = std::make_shared<DecimalV3Column<int128_t>>();
        col1->reserve(rows);
        col1->set_precision(prec);
        col1->set_scale(scale);

        auto col2 = std::make_shared<DecimalV3Column<int128_t>>();
        col2->reserve(rows);
        col2->set_precision(prec);
        col2->set_scale(scale);
        for (size_t i = 0; i < rows; ++i) {
            // note: same with scale
            col1->append(i * 1000000);
            col2->append(i * 1000000);
        }
        Columns cols{col1, col2};

        auto field1 = std::make_shared<Field>(0, "col1_decimal128", LogicalType::TYPE_DECIMAL128, prec, scale, true);
        auto field2 = std::make_shared<Field>(1, "col2_decimal128", LogicalType::TYPE_DECIMAL128, prec, scale, true);
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(col1, 0);
        chunk->append_column(col2, 1);
        return chunk;
    }

    std::vector<ChunkPtr> genAllChunks(size_t total_rows) {
        const size_t one_chunk_rows = runtime_state->chunk_size();
        size_t cur_rows = 0;
        std::vector<ChunkPtr> chunks;
        for (; cur_rows <= total_rows; ) {
            chunks.push_back(genOneChunk(one_chunk_rows));
            cur_rows += one_chunk_rows;
        }
        return chunks;
    }

    // copy stream_test.h create_stream_aggregator()
    AggregatorPtr createAggregator(
            const std::vector<std::vector<SlotTypeInfo>> & slot_infos,
            const std::vector<GroupByKeyInfo> & group_by_infos,
            const std::vector<AggInfo> & agg_infos) {
        auto params = std::make_shared<AggregatorParams>();
        // todo: meaning ?
        params->needs_finalize = true;
        params->has_outer_join_child = false;
        params->streaming_preaggregation_mode = TStreamingPreaggregationMode::AUTO;
        // todo: no intermediate_tuple
        // params->intermediate_tuple_id = 1;
        // index into slot_infos
        params->output_tuple_id = 1;
        // todo why?
        params->count_agg_idx = 0;
        params->sql_grouping_keys = "";
        params->sql_aggregate_functions = "";
        params->conjuncts = {};
        // todo why?
        // params->is_testing = true;
        // TODO: test more cases.
        params->is_append_only = false;
        // todo
        // params->is_generate_retract = is_generate_retract;
        params->grouping_exprs = _create_group_by_exprs(slot_infos[0], group_by_infos);
        params->intermediate_aggr_exprs = {};
        params->aggregate_functions = _create_agg_exprs(slot_infos[0], agg_infos);
        params->init();
        return std::make_shared<Aggregator>(params);
    }
private:
    std::vector<ChunkPtr> input_chunks;
    // todo: how to make sure it's not stream?
    AggregatorPtr aggregator;

    RuntimeState * runtime_state;
    ObjectPool obj_pool;
    RuntimeProfile * runtime_profile;
    DescriptorTbl * tbl;

    std::vector<starrocks::stream::GroupByKeyInfo> group_by_infos;
};

TEST_F(BenchAggregator, push_chunk) {
    auto * push_chunk_eval_groupby_exprs_timer = new RuntimeProfile::Counter(TUnit::TIME_NS);
    auto * push_chunk_build_hash_map_timer = new RuntimeProfile::Counter(TUnit::TIME_NS);
    auto * push_chunk_compute_agg_state_timer = new RuntimeProfile::Counter(TUnit::TIME_NS);
    auto * pull_chunk_convert_chunk = new RuntimeProfile::Counter(TUnit::TIME_NS);
    const auto chunk_size = runtime_state->chunk_size();
    for (const auto & chunk : input_chunks) {
        {
            SCOPED_TIMER(push_chunk_eval_groupby_exprs_timer);
            auto status = aggregator->evaluate_groupby_exprs(chunk.get());
            ASSERT_TRUE(status.ok());
        }
        {
            SCOPED_TIMER(push_chunk_build_hash_map_timer);
            aggregator->build_hash_map(chunk->num_rows(), /*agg_group_by_with_limit=*/false);
            aggregator->try_convert_to_two_level_map();
        }
        {
            SCOPED_TIMER(push_chunk_compute_agg_state_timer);
            // RETURN_IF_ERROR(aggregator->compute_batch_agg_states_with_selection(chunk.get(), chunk_size));
            auto status = aggregator->compute_batch_agg_states(chunk.get(), chunk_size);
            ASSERT_TRUE(status.ok());
            aggregator->update_num_input_rows(chunk_size);
            status = aggregator->check_has_error();
            ASSERT_TRUE(status.ok());
        }
    }
    {
        // todo meaning: set finish
        COUNTER_SET(aggregator->hash_table_size(), (int64_t)aggregator->hash_map_variant().size());
        // If hash map is empty, we don't need to return value
        if (aggregator->hash_map_variant().size() == 0) {
            aggregator->set_ht_eos();
        }
        aggregator->hash_map_variant().visit(
                [&](auto& hash_map_with_key) { aggregator->it_hash() = aggregator->_state_allocator.begin(); });
        COUNTER_UPDATE(aggregator->input_row_count(), aggregator->num_input_rows());
        aggregator->sink_complete();
    }

    // todo for loop to get all result.
    std::vector<ChunkPtr> chunks;
    {
        ChunkPtr chunk = std::make_shared<Chunk>();
        SCOPED_TIMER(pull_chunk_convert_chunk);
        if (aggregator->is_none_group_by_exprs()) {
            auto status = aggregator->convert_to_chunk_no_groupby(&chunk);
            ASSERT_TRUE(status.ok());
        } else {
            auto status = aggregator->convert_hash_map_to_chunk(chunk_size, &chunk);
            ASSERT_TRUE(status.ok());
        }
        chunks.push_back(chunk);
    }
    LOG(INFO) << "gjt debug push chunk eval groupby exprs: " << push_chunk_eval_groupby_exprs_timer->value();
    LOG(INFO) << "gjt debug push chunk build hash map: " << push_chunk_build_hash_map_timer->value();
    LOG(INFO) << "gjt debug push chunk compute_agg_state_timer: " << push_chunk_compute_agg_state_timer->value();

    LOG(INFO) << "gjt debug pull chunk convert chunk: " << pull_chunk_convert_chunk->value();
    LOG(INFO) << "gjt debug pull chunk iter timer: " << aggregator->iter_timer()->value();
    LOG(INFO) << "gjt debug pull chunk group by key appen: d" << aggregator->group_by_append_timer()->value();
    LOG(INFO) << "gjt debug pull chunk agg val append: " << aggregator->agg_append_timer()->value();

    // LOG(INFO) << "gjt debug agg result: ";
    // for (size_t i = 0; i < chunks.size(); ++i) {
    //     for (size_t j = 0; j < chunks[i]->num_rows(); ++j) {
    //         LOG(INFO) << chunks[i]->debug_row(j);
    //     }
    // }
}

// AggregateBaseNode::prepare() -> convert_to_aggregator_params()
// AggregatorParamsPtr genAggregatorParams() {
//     TScalarType col_scalar_type;
//     // todo decimalV2 ...
//     col_scalar_type.type = TPrimitiveType::DECIMAL128;
//     col_scalar_type.precision = 19;
//     col_scalar_type.scale = 8;
// 
//     TTypeNode col_type_node;
//     col_type_node.type = TTypeNodeType::SCALAR;
//     col_type_node.scalar_type = col_scalar_type;
// 
//     TTypeDesc col_type;
//     col_type.types.push_back(col_type_node);
// 
//     TSlotRef slot_ref;
//     slot_ref.slot_id = 1;
//     slot_ref.tuple_id = 0;
// 
//     TExprNode group_by_expr;
//     group_by_expr.node_type = TExprNodeType::SLOT_REF;
//     group_by_expr.type = col_type;
//     group_by_expr.slot_ref = slot_ref;
//     // group_by_expr.output_type?
//     // group_by_expr.output_column?
// 
//     ///// group by expr done, sum func begins
//     TScalarType sum_func_res_scalar_type;
//     // todo decimalV2 ...
//     sum_func_res_scalar_type.type = TPrimitiveType::DECIMAL128;
//     sum_func_res_scalar_type.precision = 19;
//     sum_func_res_scalar_type.scale = 8;
// 
//     TTypeNode sum_func_type_node;
//     sum_func_type_node.type = TTypeNodeType::SCALAR;
//     sum_func_type_node.scalar_type = sum_func_res_scalar_type;
// 
//     TTypeDesc sum_func_type;
//     sum_func_type.types.push_back(sum_func_type_node);
// 
//     TAggregateExpr agg_expr;
//     // only true when it's second phase of two hash agg.
//     agg_expr.is_merge_agg = false;
// 
//     TAggregateFunction agg_func;
//     // todo?
//     // agg_func.intermediate_type = ?
// 
//     TFunction sum_func;
//     sum_func.name.function_name = "sum";
//     sum_func.arg_types.push_back(col_type);
//     sum_func.ret_type = col_type;
//     sum_func.aggregate_fn = agg_func;
// 
//     TExprNode sum_func_expr_node;
//     sum_func_expr_node.node_type = TExprNodeType::AGG_EXPR;
//     sum_func_expr_node.type = sum_func_type;
//     sum_func_expr_node.num_children = 1;
//     sum_func_expr_node.agg_expr = agg_expr;
//     sum_func_expr_node.output_scale = 8;
//     sum_func_expr_node.fn = sum_func;
// 
//     TExpr sum_func_expr;
//     sum_func_expr.nodes.push_back(sum_func_expr_node);
// 
//     TAggregationNode agg_node;
//     agg_node.grouping_exprs.push_back(group_by_expr);
//     agg_node.aggregate_functions.push_back(sum_func);
//     // agg_node.output_tuple_id ?
//     agg_node.need_finalize = true;
// 
//     TPlanNode agg_plan_node;
//     agg_plan_node.node_id = 999;
//     agg_plan_node.node_type = TPlanNodeType::AGGREGATION_NODE;
//     agg_plan_node.num_children = 1;
//     // node.limit = 0; ? 
//     // node.row_tuples = 0; ?
//     // node.nullable_tuples = ?
//     // node.conjuncts = ?
//     // node.compact_data = ?
//     agg_plan_node.agg_node = agg_node;
//     // node.use_vectorized = ?
// 
//     ////// table scan node
//     TOlapScanNode tsc_node;
//     // todo:?
//     // tsc_node.tuple_id = 
// 
//     TPlanNode tsc_plan_node;
//     tsc_plan_node.node_id = 888;
//     tsc_plan_node.node_type = TPlanNodeType::OLAP_SCAN_NODE;
//     tsc_plan_node.num_children = 0;
//     tsc_plan_node.olap_scan_node = tsc_node;
// 
//     TPlan plan;
//     plan.nodes.push_back(tsc_plan_node);
//     plan.nodes.push_back(agg_plan_node);
// 
//     TPlanFragment frag;
//     frag.plan = plan;
// 
//     ////// frag params
//     TExecPlanFragmentParams req;
//     req.protocol_version = InternalServiceVersion::V1;
//     req.fragment = frag;
// 
//     auto params = std::make_shared<AggregatorParams>();
//     params->needs_finalize = true;
//     // params->intermediate_tuple_id = ?
//     // params->output_tuple_id = 
//     // params->sql_grouping_keys
//     // params->sql_aggregate_functions = 
//     params->grouping_exprs.push_back(group_by_expr);
//     params->aggregate_functions = agg_node.aggregate_functions;
//     params->intermediate_aggr_exprs = agg_node.intermediate_aggr_exprs;
// 
//     return params;
// }
} // namespace starrocks
