#include "../libstuff/sqlite3.h"
#include <string>
#include <list>
#include <vector>
#include <thread>
#include <iostream>
#include <fstream>
#include <atomic>
#include <random>
#include <sys/time.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>
using namespace std;

// Overall test settings
static int global_numQueries = 10000;
static int global_bMmap = 0;
static const char* global_dbFilename = "test.db";
static int global_cacheSize = -1000000;
static int global_querySize = 10;

// Data about the database
static uint64_t global_dbRows = 0;

// Returns the current time down to the microsecond
uint64_t STimeNow() {
    timeval time;
    gettimeofday(&time, 0);
    return ((uint64_t)time.tv_sec * 1000000 + (uint64_t)time.tv_usec);
}

// Gets the current virtual memory usage and resident set
// From: https://stackoverflow.com/a/19770392
void getMemUsage(double& vm_usage, double& resident_set)
{
    // the two fields we want
    vm_usage     = 0.0;
    resident_set = 0.0;
    unsigned long vsize;
    long rss;
    {
        std::string ignore;
        std::ifstream ifs("/proc/self/stat", std::ios_base::in);
        ifs >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore
                >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore
                >> ignore >> ignore >> vsize >> rss;
    }
    long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
    vm_usage = vsize / 1024.0;
    resident_set = rss * page_size_kb;
}

// This is called by sqlite for each row of the result
int queryCallback(void* data, int columns, char** columnText, char** columnName) {
    list<vector<string>>* results = (list<vector<string>>*)data;
    vector<string> row;
    row.reserve(columns);
    for (int i = 0; i < columns; i++) {
        row.push_back(columnText[i] ? columnText[i] : "NULL");
    }
    results->push_back(move(row));
    return SQLITE_OK;
}

// This runs a test query some number of times, optionally showing progress
void runTestQueries(sqlite3* db, int numQueries, const string& testQuery, bool showProgress) {
    // Run however many queries are requested
    for (int q=0; q<numQueries; q++) {
        // Run the query
        list<vector<string>> results;
        int error = sqlite3_exec(db, testQuery.c_str(), queryCallback, &results, 0);
        if (error != SQLITE_OK) {
            cout << "Error running test query: " << sqlite3_errmsg(db) << ", query: " << testQuery << endl;
        }

        // Optionally show progress
        if (showProgress && (q % (numQueries/10))==0 ) {
            int percent = (int)(((double)q / (double)numQueries) * 100.0);
            cout << percent << "% " << flush;
        }
    }
}

sqlite3* openDatabase() {
    // Open it per the global settings
    sqlite3* db = 0;
    if (SQLITE_OK != sqlite3_open_v2(global_dbFilename, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, "unix-none")) {
        cout << "Error: Can't open '" << global_dbFilename << "'." << endl;
        exit(1);
    }
    sqlite3_exec(db, "PRAGMA locking_mode=EXCLUSIVE;", 0, 0, 0);
    sqlite3_exec(db, "PRAGMA legacy_file_format = OFF;", 0, 0, 0);
    sqlite3_exec(db, "PRAGMA journal_mode = WAL;", 0, 0, 0);
    sqlite3_exec(db, "PRAGMA synchronous = OFF;", 0, 0, 0);
    sqlite3_exec(db, "PRAGMA count_changes = OFF;", 0, 0, 0);
    char *zSql = sqlite3_mprintf("PRAGMA cache_size(%d)", global_cacheSize);
    sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
    sqlite3_wal_autocheckpoint(db, 10000);
    if( global_bMmap ){
      sqlite3_exec(db, "PRAGMA mmap_size = 1099511627776;", 0, 0, 0); // 1TB
    } else {
      sqlite3_exec(db, "PRAGMA mmap_size = 0;", 0, 0, 0); // Disable
    }
    return db;
}

void test(int threadCount, const string& testQuery, int bConstant) {
    // Open a separate database handle for each thread
    cout << "Testing with " << threadCount << " threads: ";
    vector<sqlite3*> dbs(threadCount);
    for (int i = 0; i < threadCount; i++) {
        dbs[i] = openDatabase();
    }

    // We want to do the same number of total queries each test, but split between however
    // many threads we have
    int numQueries = global_numQueries;
    if( bConstant==0 ){
      numQueries = numQueries / threadCount; 
    }

    // Run the actual test
    auto start = STimeNow();
    list <thread> threads;
    for (int i = 0; i < threadCount; i++) {
        bool showProgress = (i == (threadCount - 1)); // Only show progress on the last thread
        threads.emplace_back(runTestQueries, dbs[i], numQueries, testQuery, showProgress);
    }
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();
    auto end = STimeNow();

    // Output the results
    double vm, rss;
    getMemUsage(vm, rss);
    cout << "Done! (" << ((end - start) / 1000000.0) << " seconds, vm=" << vm << ", rss=" << rss << ")" << endl;

    // Close all the database handles
    for (int i = 0; i < threadCount; i++) {
        sqlite3_close(dbs[i]);
    }
}


int main(int argc, char *argv[]) {
    // Disable memory status tracking as this has a known concurrency problem
    sqlite3_config(SQLITE_CONFIG_MEMSTATUS, 0);

    // Process the command line
    int maxNumThreads = 16;
    int minNumThreads = 1;
    int stepThreads = 0;

    int showStats = 0;
    int bConstant = 0;
    const char* customQuery = 0;

    struct Opt {
      int id;                     /* Value used for option in "switch" below */
      const char *z;              /* Text of option */
      int bArg;                   /* True if option requires argument */
    } aOpt[] = {
      {1,  "-numQueries",    1},
      {2,  "-cacheSize",     1},
      {4,  "-querySize",     1},
      {5,  "-maxNumThreads", 1},
      {6,  "-mmap",          0},
      {7,  "-stepThreads",   1},
      {8,  "-constantTime",  0},
      {9,  "-dbFileName",    1},
      {10, "-customQuery",   1},
      {11, "-minThreads",    1},
    };
    for (int i = 1; i < argc; i++) {
        char *z = argv[i];
        int n;
        int ii;
        int iOpt = -1;

        if( z[0]=='-' && z[1]=='-' ) z++;
        n = strlen(z);
        for(ii=0; ii<sizeof(aOpt)/sizeof(struct Opt); ii++){
          struct Opt *p = &aOpt[ii];
          if( strlen(p->z)>=n && sqlite3_strnicmp(z, p->z, n)==0 ){
            if( iOpt>=0 ){
              cerr << "ambiguous option: " << z << endl;
              return -1;
            }
            iOpt = ii;
          }
        }
        if( iOpt<0 ){
          cerr << "unknown option: " << z << endl;
          return -1;
        }
        if( aOpt[iOpt].bArg ){
          if( i==(argc-1) ){
            cerr << "option requires an argument: " << z << endl;
            return -1;
          }
          i++;
        }
        switch( aOpt[iOpt].id ){
          case 1: assert( 0==sqlite3_strnicmp("-numQueries", z, n) );
            global_numQueries = atoi(argv[i]);
            break;
          case 2: assert( 0==sqlite3_strnicmp("-cacheSize", z, n) );
            global_cacheSize = atoi(argv[i]);
            break;
          case 4: assert( 0==sqlite3_strnicmp("-querySize", z, n) );
            global_querySize = atoi(argv[i]);
            break;
          case 5: assert( 0==sqlite3_strnicmp("-maxNumThreads", z, n) );
            maxNumThreads = atoi(argv[i]);
            break;
          case 6: assert( 0==sqlite3_strnicmp("-mmap", z, n) );
            global_bMmap = 1;
            break;
          case 7: assert( 0==sqlite3_strnicmp("-stepThreads", z, n) );
            stepThreads = atoi(argv[i]);
            break;
          case 8: assert( 0==sqlite3_strnicmp("-constantTime", z, n) );
            bConstant = 1;
            break;
          case 9: assert( 0==sqlite3_strnicmp("-dbFileName", z, n) );
            global_dbFilename = argv[i];
            break;
          case 10: assert( 0==sqlite3_strnicmp("-customQuery", z, n) );
            customQuery = argv[i];
            break;
          case 11: assert( 0==sqlite3_strnicmp("-minThreads", z, n) );
            minNumThreads = atoi(argv[i]);
            if( minNumThreads<=0 ){
              cerr << "Bad value for -minThreads: " << minNumThreads << endl;
              return -1;
            }
            break;
        }
    }

    // Inspect the existing database with a full table scan, pulling it all into memory
    cout << "Precaching '" << global_dbFilename << "'...";
    sqlite3* db = openDatabase();
    string query = "SELECT COUNT(*), MIN(nonIndexedColumn) FROM perfTest;";
    list<vector<string>> results;
    auto start = STimeNow();
    int error = sqlite3_exec(db, query.c_str(), queryCallback, &results, 0);
    auto end = STimeNow();
    if (error != SQLITE_OK) {
        cout << "Error running query: " << sqlite3_errmsg(db) << ", query: " << query << endl;
    }
    global_dbRows = stoll(results.front()[0]);
    cout << "Done (" << ((end - start) / 1000000) << " seconds, " << global_dbRows << " rows)" << endl;
    sqlite3_close(db);

    // Figure out what query to use
    string testQuery;
    if (customQuery) {
        // Use the query supplied on the command line
        testQuery = customQuery;
    } else {
        // The test dataset is simply two columns filled with RANDOM(), one
        // indexed, one not.  Let's pick a random location from inside the
        // database, and then pick the next 10 rows.
        testQuery = "SELECT COUNT(*), AVG(nonIndexedColumn) FROM "
            "(SELECT nonIndexedColumn FROM perfTest WHERE indexedColumn > RANDOM() LIMIT " + to_string(global_querySize) + ");";
    }
    cout << "Testing: " << testQuery << endl;

    // Run the tests. If the user specified a -stepThreads option, run
    // the test once for each number of threads between minNumThreads and
    // maxNumThreads, inclusive. Or, if no -stepThreads option was specified,
    // run the test once with minNumThreads, then once with twice that many,
    // and so on until maxNumThreads is reached. Then run the same tests
    // again, but in the opposite order.
    //
    if( stepThreads ){
      for(int t=minNumThreads; t<=maxNumThreads; t+=stepThreads){
        test(t, testQuery, bConstant);
      }
    }else{
      for(int t=minNumThreads; t<=maxNumThreads; t=t*2){
        test(t, testQuery, bConstant);
      }
      for(int t=maxNumThreads; t>=minNumThreads; t=t/2){
        test(t, testQuery, bConstant);
      }
    }

    return 0;
}
