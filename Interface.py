#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import multiprocessing
import sys
import threading
DATABASE_NAME = 'postgres'
sys.stdout.reconfigure(encoding='utf-8')

def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def loadratings(ratingstablename, ratingsfilepath, openconnection): 
    """
    Load dữ liệu từ file .dat vào bảng PostgreSQL.
    """
    cur = openconnection.cursor()

    # Tạo bảng có các cột phụ
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            userid INTEGER,
            extra1 CHAR,
            movieid INTEGER,
            extra2 CHAR,
            rating FLOAT,
            extra3 CHAR,
            timestamp BIGINT
        );
    """)

    with open(ratingsfilepath, 'r') as f:
        cur.copy_from(f, ratingstablename, sep=':')

    # Xóa các cột phụ
    cur.execute(f"""
        ALTER TABLE {ratingstablename}
        DROP COLUMN extra1,
        DROP COLUMN extra2,
        DROP COLUMN extra3,
        DROP COLUMN timestamp;
    """)

    openconnection.commit()
    cur.close()
    # print(f"✅ Dữ liệu đã được nạp vào bảng '{ratingstablename}'.")


def process_partition(partition_id, numberofpartitions, ratingstablename, db_config):
    """
    Tiến trình con xử lý phân vùng thứ i.
    """
    delta = 5.0 / numberofpartitions
    minRange = partition_id * delta
    maxRange = minRange + delta
    table_name = f"range_part{partition_id}"

    try:
        # Kết nối CSDL riêng cho tiến trình con
        con = psycopg2.connect(**db_config)
        cur = con.cursor()

        # Kiểm tra bảng ratingstablename có tồn tại
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (ratingstablename,))
        if not cur.fetchone()[0]:
            raise Exception(f"Bảng {ratingstablename} không tồn tại!")

        # Tạo bảng phân vùng
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        cur.execute(f"""
            CREATE TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        """)

        # Tùy điều kiện để tránh trùng
        if partition_id == 0:
            condition = f"rating >= {minRange} AND rating <= {maxRange}"
        else:
            condition = f"rating > {minRange} AND rating <= {maxRange}"

        # Thực hiện chèn dữ liệu
        cur.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            SELECT userid, movieid, rating
            FROM {ratingstablename}
            WHERE {condition};
        """)

        con.commit()
        if partition_id == 0:
            print(f"[OK] Partition {table_name} done - range: [{minRange}, {maxRange}].")
        else:
            print(f"[OK] Partition {table_name} done - range: ({minRange}, {maxRange}].")
    except Exception as e:
        print(f"[ERROR] Error in partition {table_name}: {e}")
        raise  # Ném ngoại lệ thay vì chỉ in
    finally:
        if 'cur' in locals():
            cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm chính chia bảng theo range, xử lý song song từng phân vùng.
    """
    try:
        # Kiểm tra bảng đầu vào. Xác minh bảng ratingstablename tồn tại bằng truy vấn information_schema.tables.
        cursor = openconnection.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (ratingstablename,))
        if not cursor.fetchone()[0]:
            raise Exception(f"Bảng {ratingstablename} không tồn tại!")

        # Kiểm tra sự tồn tại của cột rating trong bảng qua information_schema.columns.=
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = %s AND column_name = 'rating'
            );
        """, (ratingstablename,))
        if not cursor.fetchone()[0]:
            raise Exception(f"Bảng {ratingstablename} không có cột rating!")

        # Lấy thông tin kết nối (dbname, user, password, host, port) từ openconnection bằng phương thức get_dsn_parameters().
        db_config = {
            'dbname': openconnection.get_dsn_parameters()['dbname'],
            'user': openconnection.get_dsn_parameters()['user'],
            'password': openconnection.get_dsn_parameters().get('password', '1234'),
            'host': openconnection.get_dsn_parameters().get('host', 'localhost'),
            'port': openconnection.get_dsn_parameters().get('port', 5432)
        }

        # Tạo chỉ mục rating_idx trên cột rating để tăng tốc lọc
        cursor.execute(f"CREATE INDEX IF NOT EXISTS rating_idx ON {ratingstablename}(rating);")
        openconnection.commit()

        # Khởi động tiến trình song song
        processes = []
        created_tables = []
        for i in range(numberofpartitions):
            p = multiprocessing.Process(target=process_partition, args=(i, numberofpartitions, ratingstablename, db_config))
            p.start()
            processes.append((p, i))
            created_tables.append(f"range_part{i}")

        # Kiểm tra lỗi và rollback nếu cần
        for p, partition_id in processes:
            p.join()
            if p.exitcode != 0:
                print(f"[ERROR] Partition range_part{partition_id} failed, rolling back...")
                openconnection.rollback()
                cursor = openconnection.cursor()
                for table in created_tables:
                    cursor.execute(f"DROP TABLE IF EXISTS {table};")
                openconnection.commit()
                raise Exception(f"Partition range_part{partition_id} failed, all partitions rolled back.")

        print("✅ Tất cả các phân vùng đã hoàn tất.")

    except Exception as e:
        print(f"[ERROR] Lỗi trong rangepartition: {e}")
        openconnection.rollback()
        cursor = openconnection.cursor()
        for i in range(numberofpartitions):
            cursor.execute(f"DROP TABLE IF EXISTS range_part{i};")
        openconnection.commit()
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Chèn một bản ghi vào bảng chính và bảng phân vùng range phù hợp.
    """
    try:
        # Đặt lại giao dịch nếu cần
        if openconnection.get_transaction_status() != 0:
            openconnection.rollback()
        cur = openconnection.cursor()

        # Kiểm tra bảng chính
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (ratingstablename,))
        if not cur.fetchone()[0]:
            raise Exception(f"Bảng {ratingstablename} không tồn tại!")

        # 1. Chèn vào bảng chính
        insert_main_query = f"""
            INSERT INTO {ratingstablename} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """
        cur.execute(insert_main_query, (userid, itemid, rating))

        # 2. Lấy số lượng phân vùng
        cur.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name LIKE 'range_part%';
        """)
        num_partitions = cur.fetchone()[0]
        if num_partitions <= 0:
            raise Exception("❌ Không tìm thấy phân vùng range_part!")

        # 3. Tính toán phân vùng trực tiếp
        min_rating = 0.0
        max_rating = 5.0
        delta = (max_rating - min_rating) / num_partitions
        if rating == min_rating:
            partition = 0
        else:
            partition = None
            for i in range(num_partitions):
                lower_bound = min_rating + i * delta
                upper_bound = min_rating + (i + 1) * delta

                if i == 0:
                    if lower_bound <= rating <= upper_bound:
                        partition = i
                        break
                else:
                    if lower_bound < rating <= upper_bound:
                        partition = i
                        break
            if partition is None:
                # Nếu không khớp, gán vào partition cuối
                partition = num_partitions - 1 
        partition_table = f"range_part{partition}"

        # Kiểm tra bảng phân vùng
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (partition_table,))
        if not cur.fetchone()[0]:
            raise Exception(f"Bảng phân vùng {partition_table} không tồn tại!")

        # 4. Chèn vào bảng phân vùng
        insert_partition_query = f"""
            INSERT INTO {partition_table} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """
        cur.execute(insert_partition_query, (userid, itemid, rating))

        # 5. Lưu thay đổi
        openconnection.commit()
        print(f"[OK] Đã chèn bản ghi vào {ratingstablename} và {partition_table}.")

    except Exception as e:
        print(f"[ERROR] Lỗi trong rangeinsert: {e}")
        openconnection.rollback()
        raise
    finally:
        if 'cur' in locals():
            cur.close()

import io


def insert_to_partition(ratingstablename, partition_index, numberofpartitions, openconnection, RROBIN_TABLE_PREFIX):
    """
    Hàm phụ để chèn dữ liệu vào một phân vùng cụ thể (chạy trong luồng riêng).
    """
    try:
        # Tạo kết nối riêng cho luồng
        con = getopenconnection()
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

        # Chèn dữ liệu vào phân vùng
        cur.execute(f"""
            INSERT INTO {RROBIN_TABLE_PREFIX}{partition_index} (userid, movieid, rating)
            SELECT userid, movieid, rating
            FROM (
                SELECT userid, movieid, rating,
                       (ROW_NUMBER() OVER () - 1) % {numberofpartitions} AS partition_index
                FROM {ratingstablename}
            ) AS temp
            WHERE partition_index = {partition_index};
        """)

        con.commit()
    except Exception as e:
        con.rollback()
        raise Exception(f"Error in insert_to_partition {partition_index}: {str(e)}")
    finally:
        cur.close()
        con.close()




def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Tạo N phân vùng ngang của bảng Ratings sử dụng phương pháp round-robin với đa luồng.
    """
    try:
        con = openconnection
        cur = con.cursor()
        RROBIN_TABLE_PREFIX = 'rrobin_part'

        # Kiểm tra đầu vào
        if not isinstance(numberofpartitions, int) or numberofpartitions <= 0:
            raise ValueError("numberofpartitions must be a positive integer")

        # Tạo bảng meta-data với khóa chính
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rr_metadata (
                table_name VARCHAR(50) PRIMARY KEY,
                num_partitions INTEGER
            );
        """)

        # Xóa các phân vùng cũ và meta-data cũ
        cur.execute("DELETE FROM rr_metadata WHERE table_name = %s;", (ratingstablename,))
        for i in range(numberofpartitions):
            table_name = f"{RROBIN_TABLE_PREFIX}{i}"
            cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")

            # Tạo bảng phân vùng
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    userid INTEGER,
                    movieid INTEGER,
                    rating FLOAT
                );
            """)

        con.commit()

        # Tạo danh sách các luồng để chèn dữ liệu vào phân vùng
        threads = []
        for i in range(numberofpartitions):
            thread = threading.Thread(
                target=insert_to_partition,
                args=(ratingstablename, i, numberofpartitions, openconnection, RROBIN_TABLE_PREFIX)
            )
            threads.append(thread)
            thread.start()

        # Đợi tất cả các luồng hoàn thành
        for thread in threads:
            thread.join()

        # Lưu meta-data
        cur.execute("""
            INSERT INTO rr_metadata (table_name, num_partitions)
            VALUES (%s, %s)
            ON CONFLICT (table_name) DO UPDATE 
            SET num_partitions = EXCLUDED.num_partitions;
        """, (ratingstablename, numberofpartitions))

        con.commit()

    except Exception as e:
        con.rollback()
        raise Exception(f"Error in roundrobinpartition: {str(e)}")
    finally:
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Chèn một bản ghi mới vào bảng Ratings và phân vùng round-robin phù hợp.
    """
    try:
        con = openconnection
        cur = con.cursor()
        RROBIN_TABLE_PREFIX = 'rrobin_part'

        # Kiểm tra xem rr_metadata có tồn tại không
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'rr_metadata'
            );
        """)
        metadata_exists = cur.fetchone()[0]

        if not metadata_exists:
            raise Exception("rr_metadata table does not exist. Run roundrobinpartition first.")

        # Lấy num_partitions từ rr_metadata
        cur.execute("""
            SELECT num_partitions FROM rr_metadata WHERE table_name = %s;
        """, (ratingstablename,))
        result = cur.fetchone()
        if not result:
            raise Exception(f"No partition metadata found for table {ratingstablename}. Run roundrobinpartition first.")
        numberofpartitions = result[0]

        # Chèn vào bảng gốc
        cur.execute(f"""
            INSERT INTO {ratingstablename} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))

        # Lấy tổng số bản ghi
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
        total_rows = cur.fetchone()[0]

        # Tính phân vùng đích
        partition_index = (total_rows - 1) % numberofpartitions
        table_name = f"{RROBIN_TABLE_PREFIX}{partition_index}"

        # Chèn vào phân vùng
        cur.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))

        con.commit()

    except Exception as e:
        con.rollback()
        raise Exception(f"Error in roundrobininsert: {str(e)}")
    finally:
        cur.close()
# def create_db(dbname):
#     """
#     We create a DB by connecting to the default user and database of Postgres
#     The function first checks if an existing database exists for a given name, else creates it.
#     :return:None
#     """
#     # Connect to the default database
#     con = getopenconnection(dbname='postgres')
#     con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
#     cur = con.cursor()

#     # Check if an existing database with the same name exists
#     cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
#     count = cur.fetchone()[0]
#     if count == 0:
#         cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
#     else:
#         print('A database named {0} already exists'.format(dbname))

#     # Clean up
#     cur.close()
#     con.close()

# def count_partitions(prefix, openconnection):
#     """
#     Function to count the number of tables which have the @prefix in their name somewhere.
#     """
#     con = openconnection
#     cur = con.cursor()
#     cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
#     count = cur.fetchone()[0]
#     cur.close()

#     return count
