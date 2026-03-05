
def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"
    
def test_channel_handle(channel_handle):
    assert channel_handle == "MOCK_CHANNEL_HANDLE123"
    
def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name"
    
def test_dags_integrity(dag_bag):
    # 1. Check for DAG import errors
    assert dag_bag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"   
    print("========")
    print(dag_bag.import_errors)
    # 2. Check that expected DAGs are present
    expected_dags = ["produce_json", "update_db", "data_quality_check"]
    loaded_dags = list(dag_bag.dags.keys())
    print("========")
    print(dag_bag.dags.keys())  
    
    for dag_id in expected_dags:
        assert dag_id in loaded_dags, f"DAG '{dag_id}' not found in DAG bag"
        
    # 3. Check that the total number of DAGs is as expected
    assert dag_bag.size() == 3
    print("======")
    print(dag_bag.size())
    
    expected_task_counts = {
        "produce_json": 5,
        "update_db": 3,
        "data_quality_check": 2   }
    
    print("========")
    for dag_id, dag in dag_bag.dags.items():
        expected_count = expected_task_counts[dag_id]
        actual_count = len(dag.tasks)
        assert (
            expected_count == actual_count
        ), "DAG '{dag_id}' has {actual_count} tasks, expected {expected_count}"
        print(dag_id, len(dag.tasks))