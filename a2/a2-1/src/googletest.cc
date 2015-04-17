#ifndef TEST1_H
#define TEST1_H
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
#include "File.h"
#include "DBFile.h"
#include "test.h"
#include "BigQ.h"
#include <pthread.h>
#include "gtest/gtest.h"


using namespace std;

OrderMaker sortorder;

void *producer (void *arg) {

	Pipe *myPipe = (Pipe *) arg;

	Record temp;
	int counter = 0;

	DBFile dbfile;
	dbfile.Open (rel->path ());
	cout << " producer: opened DBFile " << rel->path () << endl;
	dbfile.MoveFirst ();

	while (dbfile.GetNext (temp) == 1) {
		counter += 1;
		if (counter%100000 == 0) {
			 cerr << " producer: " << counter << endl;	
		}
		myPipe->Insert (&temp);
	}

	dbfile.Close ();
	myPipe->ShutDown ();

	cout << " producer: inserted " << counter << " recs into the pipe\n";
}

void *consumer (void *arg) {
	
	testutil *t = (testutil *) arg;

	ComparisonEngine ceng;

	DBFile dbfile;
	char outfile[100];

	if (t->write) {
		sprintf (outfile, "%s.bigq", rel->path ());
		dbfile.Create (outfile, heap, NULL);
	}

	int err = 0;
	int i = 0;

	Record rec[2];
	Record *last = NULL, *prev = NULL;

	while (t->pipe->Remove (&rec[i%2])) {
		prev = last;
		last = &rec[i%2];

		if (prev && last) {
			if (ceng.Compare (prev, last, t->order) == 1) {
				prev->Print(rel->schema());
				last->Print(rel->schema());
				err++;
			}
			if (t->write) {
				dbfile.Add (*prev);
			}
		}
		if (t->print) {
			last->Print (rel->schema ());
		}
		i++;
	}

	cout << " consumer: removed " << i << " recs from the pipe\n";

	if (t->write) {
		if (last) {
			dbfile.Add (*last);
		}
		cerr << " consumer: recs removed written out as heap DBFile at " << outfile << endl;
		dbfile.Close ();
	}
	cerr << " consumer: " << (i - err) << " recs out of " << i << " recs in sorted order \n";
	if (err) {
		cerr << " consumer: " <<  err << " recs failed sorted order test \n" << endl;
	}
}


void test1 (int runlen) {

	// sort order for records

	int buffsz = 100; // pipe cache size
	Pipe input (buffsz);
	Pipe output (buffsz);

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread1;
	pthread_create (&thread1, NULL, producer, (void *)&input);

	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	testutil tutil = {&output, &sortorder, false, false};
	tutil.write = true;
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);

	BigQ bq (input, output, sortorder, runlen);

	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
}

class DbiEnvironment : public testing::Environment{
	public:
    virtual void SetUp()
    {
    	setup ();

		relation *rel_ptr[] = {n, r, c, p, ps, o, li, s};

		int findx = 0;
		while (findx < 1 || findx > 8) {
			cout << "\n select dbfile to use: \n";
			cout << "\t 1. nation \n";
			cout << "\t 2. region \n";
			cout << "\t 3. customer \n";
			cout << "\t 4. part \n";
			cout << "\t 5. partsupp \n";
			cout << "\t 6. orders \n";
			cout << "\t 7. lineitem \n";
			cout << "\t 8. supplier \n \t ";
			cin >> findx;
		}
		rel = rel_ptr [findx - 1];
		
		int runlen;
		cout << "\t\n specify runlength:\n\t ";
		cin >> runlen;

		rel->get_sort_order (sortorder);
		
		//sort + write
		test1 ( runlen);
		

        cout << "DbiEnvironment SetUP" << endl;
    }
    virtual void TearDown()
    {
        
		cleanup ();
        cout << "DbiEnvironment TearDown" << endl;
    }
};



// check num of recs in dbfile = num of recs in sorted file
TEST(A2_1Test, CheckNumOfRecs){

		DBFile bigqfile;
		DBFile dbfile;

		//Open both of dbfile and bigqfile
		char outfile[100];
		sprintf (outfile, "%s.bigq", rel->path ());
		bigqfile.Open (outfile);
		dbfile.Open(rel->path ());

		//Compare if their num of recs are the same
		EXPECT_EQ(bigqfile.GetRecNum(), dbfile.GetRecNum());

		//Close them
		bigqfile.Close();
		dbfile.Close();
}

// Scan the recs in sorted file to check if (rec) <= (rec++)
TEST(A2_1Test, CheckOrder){
	//open bigq
	DBFile bigqfile;

	char outfile[100];
	sprintf (outfile, "%s.bigq", rel->path ());
	bigqfile.Open (outfile);

	//movefirst
	bigqfile.MoveFirst();

	//getnext prev last
	Record prev;
	Record last;
	if( !(bigqfile.GetNext(prev) && bigqfile.GetNext(last)) ){
		cerr << "Error! Less than 2 records."  << endl;
		exit(1);
	}

	// check prev <= last 
	ComparisonEngine ceng;

	EXPECT_FALSE(ceng.Compare (&prev, &last, &sortorder) > 0);

	//move to next until done
	int flag1 = 1;
	int flag2 = 1;
	while(flag1 && flag2){
		
		flag1 = bigqfile.GetNext(prev);
		if(flag1)
			EXPECT_FALSE(ceng.Compare (&last, &prev, &sortorder) > 0);

		flag2 = bigqfile.GetNext(last);
		if(flag2)
			EXPECT_FALSE(ceng.Compare (&prev, &last, &sortorder) > 0);
	}
}




int main(int argc, char **argv) {


	testing::AddGlobalTestEnvironment(new DbiEnvironment());
    ::testing::InitGoogleTest(&argc, argv); 

    return RUN_ALL_TESTS();
}

#endif