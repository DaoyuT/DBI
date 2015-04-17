
#include <iostream>
#include "Record.h"
#include <stdlib.h>
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main () {

	// try to parse the CNF
	/*cout << "Enter in your CNF: ";
  	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}

	// suck up the schema from the file
	//Schema lineitem ("catalog", "lineitem");

	// grow the CNF expression from the parse tree 
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &lineitem, literal);
	
	// print out the comparison to the screen
	myComparison.Print ();*/

	// now open up the text file and start procesing it
    FILE *tableFile = fopen ("/home/yogesh/dbi/a1-working/data/lineitem.tbl.bkp", "r");

    Record temp;
    Schema mySchema ("catalog", "lineitem");

	//char *bits = literal.GetBits ();
	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	//literal.Print (&supplier);

    // read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	/*int counter = 0;
	ComparisonEngine comp;
        while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}

		//if (comp.Compare (&temp, &literal, &myComparison))
                	temp.Print (&mySchema);

        }*/

    /*while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
    	temp.PrintBits(&mySchema);
    	temp.Print(&mySchema);
    }*/

    File f;
    f.Open(0, "/home/yogesh/dbi/a1-working/dbfile/test.db");
    Page *p = new Page();

  	off_t pageNo = 0;
  	bool pageWritten = false;
  	int numRec = 0;
    cout<<"Curr File Len : "<<f.GetLength()<<endl;
    while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
    	numRec++;
    	if(!p->Append(&temp)){
    		f.AddPage(p, pageNo);
            //cout<<"Curr File Len : "<<f.GetLength()<<endl;
    		pageWritten = true;
    		p->EmptyItOut();
    		p->Append(&temp);
    		pageNo++;
    	}else{
    		pageWritten = false;
    	}
    	//cout<<p->Append(&temp)<<endl;
    }
    if(!pageWritten) f.AddPage(p, pageNo);
    cout<<"Number of records written to page in file: "<<p->NumRecords()<<endl;
    cout<<"File length: "<<f.GetLength()<<endl;
    delete p;
    f.Close();

    f.Open(1, "/home/yogesh/dbi/a1-working/dbfile/test.db");
    cout<<"File length: "<<f.GetLength()<<endl;

    p = new Page(); 
    pageNo = 0;
    f.GetPage(p, pageNo);
    cout<<"Number of records in page read from file: "<<p->NumRecords()<<endl;
    if(p->GetFirst(&temp)){
        temp.Print(&mySchema);
    }

    Page *p2 = new Page(); 
    pageNo = 1;
    f.GetPage(p2, pageNo);
    cout<<"Number of records in page read from file: "<<p2->NumRecords()<<endl;
    if(p2->GetFirst(&temp)){
        temp.Print(&mySchema);
    }
    //p->GetFirst(&temp);
    //temp.Print(&mySchema);
}