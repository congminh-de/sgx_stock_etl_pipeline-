INSERT IGNORE INTO meta_file_status (file_id, status)
SELECT COALESCE(MAX(file_id), 6075) + 1, 'PENDING'
FROM meta_file_status;
