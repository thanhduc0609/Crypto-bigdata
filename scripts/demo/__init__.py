"""
Crypto Big Data - Demo & Local Operations
=========================================
Quản lý vòng đời dịch vụ demo, supervisor, container và kiểm tra độ tin cậy:
- setup_env.ps1 (-StartDemo / -StopDemo): Điều phối khởi động và dừng toàn bộ dịch vụ
- docker_environment.py: Quản lý Docker container
- local_service.py: Supervisor tiến trình con (Web, Producer, Spark)
- monitor_demo.py: Giám sát pipeline thời gian thực
- check_demo_recovery.py: Kiểm thử khả năng phục hồi sau sự cố
- demo_load_check.py: Đánh giá độ trễ và tải cục bộ
- audit_demo_pipeline.py: Kiểm toán chuỗi xử lý đầu cuối
- export_demo_handover.py: Xuất biên bản bàn giao
"""

