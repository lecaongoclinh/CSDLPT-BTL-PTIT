#
# Tester for the assignement1
#
DATABASE_NAME = 'postgres'

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'ratings.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 10000054  # Number of lines in the input file
import time
import psycopg2
import traceback
import testHelper
import Interface as MyAssignment
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
if __name__ == '__main__':
    start_time = time.time()
    try:
        testHelper.createdb(DATABASE_NAME)

        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            testHelper.deleteAllPublicTables(conn)

            [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
            if result :
                print("loadratings function pass!")
            else:
                print("loadratings function fail!")

            [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            if result :
                print("rangepartition function pass!")
            else:
                print("rangepartition function fail!")

            # ALERT:: Use only one at a time i.e. uncomment only one line at a time and run the script
            # [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 3, conn, '2')
            [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 0, conn, '0')
            if result:
                print("rangeinsert function pass!")
            else:
                print("rangeinsert function fail!")

            testHelper.deleteAllPublicTables(conn)
            MyAssignment.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)

            [result, e] = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            if result :
                print("roundrobinpartition function pass!")
            else:
                print("roundrobinpartition function fail")

            # ALERT:: Change the partition index according to your testing sequence.
            [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '4')
            # [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '1')
            # [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '2')
            if result :
                print("roundrobininsert function pass!")
            else:
                print("roundrobininsert function fail!")

    except Exception as detail:
        traceback.print_exc()
    finally:
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"⏱️ Thời gian chạy: {elapsed_time:.2f} giây")

