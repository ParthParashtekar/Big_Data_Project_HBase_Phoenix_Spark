
DELETE FROM smart_telemetry_crud_test
WHERE salt_bucket = 0
  AND serial_number = 'CRUD_TEST_99'
  AND event_date = DATE '2024-04-01';
