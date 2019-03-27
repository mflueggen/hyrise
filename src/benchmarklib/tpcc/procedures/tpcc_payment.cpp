#include <ctime>
#include <random>

#include "tpcc_payment.hpp"

namespace opossum {

TpccPayment::TpccPayment(const int num_warehouses) {
  // TODO this should be [1, n], but our data generator does [0, n-1]
  std::uniform_int_distribution<> warehouse_dist{0, num_warehouses - 1};
	_w_id = warehouse_dist(_random_engine);

  // There are always exactly 9 districts per warehouse
  // TODO this should be [1, 10], but our data generator does [0, 9]
  std::uniform_int_distribution<> district_dist{0, 9};
	_d_id = district_dist(_random_engine);

  // Use home warehouse in 85% of cases, otherwise select a random one
  // TODO Deal with single warehouse
  std::uniform_int_distribution<> home_warehouse_dist{1, 100};
  _c_w_id = _w_id;
  _c_d_id = _d_id;
  if (home_warehouse_dist(_random_engine) > 85) {
    do {
      _c_w_id = warehouse_dist(_random_engine);
    } while (_c_w_id == _w_id);
  _c_d_id = district_dist(_random_engine);
  }

  // Select 6 out of 10 customers by last name
  std::uniform_int_distribution<> customer_selection_method_dist{1, 10};
  _select_customer_by_name = customer_selection_method_dist(_random_engine) <= 6;
  if (_select_customer_by_name) {
    _customer = pmr_string{_tpcc_random_generator.last_name(_tpcc_random_generator.nurand(255, 0, 999))};
  } else {
  // TODO this should be [1, 3000], but our data generator does [0, 2999]
    _customer = _tpcc_random_generator.nurand(1023, 0, 2999);
  }

  // Generate payment information
  std::uniform_real_distribution<float> amount_dist{1.f, 5000.f};
  _h_amount = amount_dist(_random_engine);
  _h_date = std::time(nullptr);
}

void TpccPayment::execute() {
  // Retrieve information about the warehouse
  const auto warehouse_table = _execute_sql(std::string{"SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_YTD FROM WAREHOUSE WHERE W_ID = "} + std::to_string(_w_id));
  Assert(warehouse_table->row_count() == 1, "Did not find warehouse (or found more than one)");
  _w_name = warehouse_table->get_value<pmr_string>(ColumnID{0}, 0);
  _w_ytd = warehouse_table->get_value<float>(ColumnID{6}, 0);

  // Update warehouse YTD
  _execute_sql(std::string{"UPDATE WAREHOUSE SET W_YTD = "} + std::to_string(_w_ytd + _h_amount) + " WHERE W_ID = " + std::to_string(_w_id));

  // Retrieve information about the district
  const auto district_table = _execute_sql(std::string{"SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_YTD FROM DISTRICT WHERE D_W_ID = "} + std::to_string(_w_id) + " AND D_ID = " + std::to_string(_d_id));
  Assert(district_table->row_count() == 1, "Did not find district (or found more than one)");
  _d_name = district_table->get_value<pmr_string>(ColumnID{0}, 0);
  _d_ytd = district_table->get_value<float>(ColumnID{6}, 0);

  // Update district YTD
  _execute_sql(std::string{"UPDATE DISTRICT SET D_YTD = "} + std::to_string(_d_ytd + _h_amount) + " WHERE D_W_ID = " + std::to_string(_w_id) + " AND D_ID = " + std::to_string(_d_id));

  auto customer_table = std::shared_ptr<const Table>{};
  auto customer_offset = size_t{};
  auto customer_id = int32_t{};

  if (!_select_customer_by_name) {
    // Case 1 - Select customer by ID
    customer_table = _execute_sql(std::string{"SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA FROM CUSTOMER WHERE C_W_ID = "} + std::to_string(_w_id) + " AND C_D_ID = " + std::to_string(_c_d_id) + " AND C_ID = " + std::to_string(std::get<int32_t>(_customer)));
    Assert(customer_table->row_count() == 1, "Did not find customer by ID (or found more than one)");

    customer_offset = size_t{0};
    customer_id = std::get<int32_t>(_customer);
  } else {
    // Case 2 - Select customer by name
    customer_table = _execute_sql(std::string{"SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA FROM CUSTOMER WHERE C_W_ID = "} + std::to_string(_w_id) + " AND C_D_ID = " + std::to_string(_c_d_id) + " AND C_LAST = '" + std::string{std::get<pmr_string>(_customer)} + "' ORDER BY C_FIRST");
    Assert(customer_table->row_count() >= 1, "Did not find customer by name");

    // Calculate ceil(n/2)
    customer_offset = static_cast<size_t>(std::min(std::ceil(customer_table->row_count() / 2.0), static_cast<double>(customer_table->row_count() - 1)));
    customer_id = customer_table->get_value<int32_t>(ColumnID{0}, customer_offset);
  }

  // There is a possible optimization here if we take `customer_table` as an input to an UPDATE operator, but that would be outside of the SQL realm. Also, it would it make it impossible to run _execute_sql via the network layer later on.
  _execute_sql(std::string{"UPDATE CUSTOMER SET C_BALANCE = C_BALANCE - "} + std::to_string(_h_amount) + ", C_YTD_PAYMENT = C_YTD_PAYMENT + " + std::to_string(_h_amount) + ", C_PAYMENT_CNT = C_PAYMENT_CNT + 1 WHERE C_W_ID = " + std::to_string(_w_id) + " AND C_D_ID = " + std::to_string(_c_d_id) + " AND C_ID = " + std::to_string(customer_id));

  // Retrieve C_CREDIT and check for "bad credit"
  if (customer_table->get_value<pmr_string>(ColumnID{11}, customer_offset) == "BC") {
    std::stringstream new_c_data_stream;
    new_c_data_stream << customer_table->get_value<int32_t>(ColumnID{0}, customer_offset);  // C_ID
    new_c_data_stream << _c_d_id;
    new_c_data_stream << _c_w_id;
    new_c_data_stream << _d_id;
    new_c_data_stream << _w_id;
    new_c_data_stream << _h_amount;
    new_c_data_stream << customer_table->get_value<pmr_string>(ColumnID{15}, customer_offset);  // C_DATA
    auto new_c_data = new_c_data_stream.str();
    new_c_data.resize(std::min(new_c_data.size(), size_t{500}));
    _execute_sql(std::string{"UPDATE CUSTOMER SET C_DATA = '"} + new_c_data + "' WHERE C_W_ID = " + std::to_string(_w_id) + " AND C_D_ID = " + std::to_string(_c_d_id) + " AND C_ID = " + std::to_string(customer_id));
  }

  // Insert into history table
  // TODO - why is HISTORY a keyword?
  _execute_sql(std::string{"INSERT INTO \"HISTORY\" (H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATA, H_DATE, H_AMOUNT) VALUES (" + std::to_string(customer_id) + ", " + std::to_string(_c_d_id) + ", " + std::to_string(_c_w_id) + ", " + std::to_string(_d_id) + ", " + std::to_string(_w_id) + ", '" + std::string{_w_name + "    " + _d_name} + "', '" + std::to_string(_h_date) + "', " + std::to_string(_h_amount) + ")"});

  _transaction_context->commit();
}

char TpccPayment::identifier() const { return 'P'; }

std::ostream& TpccPayment::print(std::ostream& stream) const {
  stream << "Payment";
  return stream;
}

}
