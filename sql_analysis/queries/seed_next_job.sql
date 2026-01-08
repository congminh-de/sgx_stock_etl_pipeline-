INSERT IGNORE INTO meta_file_status VALUES (file_id, status)
SELECT MAX(file_id) + 1, 'PENDING'
FROM meta_file_status;
