Build

     cargo build --release
     
Run

(Double quote glob pattern to protect them from expansion by the shell)

    target/release/whatf "/home/dave/place-for-logs/i-*/httpd-access/access.*.log.gz"
    
...outputs by_status_timeslice.tsv and by_uritype_timeslice.tsv  Timeslices hardcoded to 5 minutes.
