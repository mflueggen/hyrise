#include <ctime>
#include <random>

#include "tpcc_delivery.hpp"

namespace opossum {

TpccDelivery::TpccDelivery(const int num_warehouses, BenchmarkSQLExecutor sql_executor) : AbstractTpccProcedure(sql_executor) {
  // TODO this should be [1, n], but our data generator does [0, n-1]
  std::uniform_int_distribution<> warehouse_dist{0, num_warehouses - 1};
	_w_id = warehouse_dist(_random_engine);

  std::uniform_int_distribution<> carrier_dist{1, 10};
  _o_carrier_id = carrier_dist(_random_engine);

  _ol_delivery_d = std::time(nullptr);
}

bool TpccDelivery::execute() {
  for (auto d_id = 1; d_id <= 10; ++d_id) {
    // This could be optimized by querying only once and grouping by NO_D_ID
    const auto new_order_select_pair = _sql_executor.execute(std::string{"SELECT COUNT(*), MIN(NO_O_ID) FROM NEW_ORDER WHERE NO_W_ID = "} + std::to_string(_w_id) + " AND NO_D_ID = " + std::to_string(d_id));
    const auto& new_order_table = new_order_select_pair.second;

    // TODO(anyone): this would not be necessary if get_value returned NULLs as nullopt
    if (new_order_table->get_value<int64_t>(ColumnID{0}, 0) == 0) continue;

    const auto no_o_id = new_order_table->get_value<int32_t>(ColumnID{1}, 0);

    // Delete from NEW_ORDER
    const auto new_order_update_pair = _sql_executor.execute(std::string{"DELETE FROM NEW_ORDER WHERE NO_W_ID = "} + std::to_string(_w_id) + " AND NO_D_ID = " + std::to_string(d_id) + " AND NO_O_ID = " + std::to_string(no_o_id));
    if (new_order_update_pair.first != SQLPipelineStatus::Success) {_sql_executor.rollback(); return false;}

    // Get customer ID
    const auto order_select_pair = _sql_executor.execute(std::string{"SELECT O_C_ID FROM \"ORDER\" WHERE O_W_ID = "} + std::to_string(_w_id) + " AND O_D_ID = " + std::to_string(d_id) + " AND O_ID = " + std::to_string(no_o_id));
    const auto& order_table = order_select_pair.second;
    Assert(order_table->row_count() == 1, "Did not find order");
    auto o_c_id = order_table->get_value<int32_t>(ColumnID{0}, 0);

    // Update ORDER
    const auto order_update_pair = _sql_executor.execute(std::string{"UPDATE \"ORDER\" SET O_CARRIER_ID = "} + std::to_string(_o_carrier_id) + " WHERE O_W_ID = " + std::to_string(_w_id) + " AND O_D_ID = " + std::to_string(d_id) + " AND O_ID = " + std::to_string(no_o_id));
    if (order_update_pair.first != SQLPipelineStatus::Success) {_sql_executor.rollback(); return false;}

    // Retrieve amount from ORDER_LINE
    const auto order_line_select_pair = _sql_executor.execute(std::string{"SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_W_ID = "} + std::to_string(_w_id) + " AND OL_D_ID = " + std::to_string(d_id) + " AND OL_O_ID = " + std::to_string(no_o_id));
    const auto& order_line_table = order_line_select_pair.second;
    Assert(order_line_table->row_count() == 1, "Did not find order lines");
    auto amount = order_line_table->get_value<double>(ColumnID{0}, 0);

    // Set delivery date in ORDER_LINE
    const auto order_line_update_pair = _sql_executor.execute(std::string{"UPDATE ORDER_LINE SET OL_DELIVERY_D = "} + std::to_string(_ol_delivery_d) + " WHERE OL_W_ID = " + std::to_string(_w_id) + " AND OL_D_ID = " + std::to_string(d_id) + " AND OL_O_ID = " + std::to_string(no_o_id));
    if (order_line_update_pair.first != SQLPipelineStatus::Success) {_sql_executor.rollback(); return false;}

    // Update balance and delivery count for customer
    const auto customer_update_pair = _sql_executor.execute(std::string{"UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + "} + std::to_string(amount) + ", C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE C_W_ID = " + std::to_string(_w_id) + " AND C_D_ID = " + std::to_string(d_id) + " AND C_ID = " + std::to_string(o_c_id));
    if (customer_update_pair.first != SQLPipelineStatus::Success) {_sql_executor.rollback(); return false;}
  }

  _sql_executor.commit();
  return true;
}

char TpccDelivery::identifier() const { return 'D'; }

}
