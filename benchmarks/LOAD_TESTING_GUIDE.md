# Load Testing Configuration for FastMSSQL

## Connection Strings Examples

### SQL Server with Windows Authentication
```
Server=localhost;Database=test;Integrated Security=true;TrustServerCertificate=yes
```

### SQL Server with SQL Authentication
```
Server=localhost,1433;Database=test;User Id=sa;Password=YourPassword;TrustServerCertificate=yes
```

### SQL Server Express
```
Server=localhost\\SQLEXPRESS;Database=test;Integrated Security=true;TrustServerCertificate=yes
```

### Azure SQL Database
```
Server=your-server.database.windows.net;Database=your-db;User Id=your-user;Password=your-password;Encrypt=true
```

## Test Scenarios

### 1. Basic Load Test
Tests basic functionality with low concurrent load.
```bash
python quick_load_test.py "YOUR_CONNECTION_STRING" --scenario basic
```

### 2. Medium Load Test
Tests with moderate concurrent load.
```bash
python quick_load_test.py "YOUR_CONNECTION_STRING" --scenario medium
```

### 3. Heavy Load Test
Tests with high concurrent load to find limits.
```bash
python quick_load_test.py "YOUR_CONNECTION_STRING" --scenario heavy
```

### 4. Extreme Load Test
Pushes the library to its absolute limits.
```bash
python quick_load_test.py "YOUR_CONNECTION_STRING" --scenario extreme
```

## Full Load Test Suite

### Complete Testing
```bash
python load_test.py --connection-string "YOUR_CONNECTION_STRING" --test-type full --max-workers 100 --duration 60
```

### Ramp-up Testing Only
```bash
python load_test.py --connection-string "YOUR_CONNECTION_STRING" --test-type ramp-up --max-workers 200 --duration 30
```

### Stress Testing Only
```bash
python load_test.py --connection-string "YOUR_CONNECTION_STRING" --test-type stress --duration 45
```

## Performance Tuning Tips

### Pool Configuration for High Throughput
- `max_size`: Set to 2-4x your expected concurrent workers
- `min_idle`: Keep some connections warm (10-20% of max_size)
- `connection_timeout_secs`: Lower for fail-fast behavior (5-10s)
- `idle_timeout_secs`: Balance between performance and resource usage

### SQL Server Configuration
- Ensure sufficient `max worker threads`
- Monitor `max degree of parallelism` (MAXDOP)
- Check connection limits and memory allocation
- Consider connection pooling at the server level

### System Resources
- Monitor CPU usage during tests
- Watch memory consumption patterns
- Check network bandwidth utilization
- Verify disk I/O is not a bottleneck

## Expected Performance Ranges

### Typical Results (varies by hardware/network):
- **Simple SELECT queries**: 500-5000 RPS
- **Parameterized queries**: 300-2000 RPS  
- **Complex JOINs**: 100-800 RPS
- **INSERT operations**: 200-1500 RPS

### Warning Signs:
- Error rate > 5%: Connection pool too small or server overloaded
- Response time > 100ms: Network latency or server performance issues
- Memory usage > 500MB: Potential memory leaks or excessive buffering

## Troubleshooting

### Common Issues:
1. **Connection timeouts**: Increase pool size or connection timeout
2. **High error rates**: Check SQL Server connection limits
3. **Memory growth**: Verify result sets are being properly consumed
4. **Poor RPS**: Check query performance and indexing

### SQL Server Monitoring Queries:
```sql
-- Check active connections
SELECT COUNT(*) as active_connections FROM sys.dm_exec_sessions WHERE is_user_process = 1

-- Check waiting tasks
SELECT * FROM sys.dm_os_waiting_tasks WHERE wait_type NOT LIKE '%SLEEP%'

-- Check resource usage
SELECT * FROM sys.dm_db_resource_stats ORDER BY start_time DESC
```
