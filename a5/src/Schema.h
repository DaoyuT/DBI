
#ifndef SCHEMA_H
#define SCHEMA_H

#include <stdio.h>
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

struct att_pair {
	char *name;
	Type type;
};
struct Attribute {

	char *name;
	Type myType;
};

class OrderMaker;
class Schema {


	// gives the attributes in the schema
	int numAtts;
	Attribute *myAtts;

	// gives the physical location of the binary file storing the relation
	char *fileName;

	friend class Record;

public:

	// gets the set of attributes, but be careful with this, since it leads
	// to aliasing!!!
	Attribute *GetAtts ();

	// returns the number of attributes
	int GetNumAtts ();

	// this finds the position of the specified attribute in the schema
	// returns a -1 if the attribute is not present in the schema
	int Find (char *attName);

	// this finds the type of the given attribute
	Type FindType (char *attName);

    // deep copy
    Schema (const Schema& sch);

	// this reads the specification for the schema in from a file
	Schema (char *fName, char *relName, const char* alias = "");

	// this composes a schema instance in-memory
	Schema (char *fName, int num_atts, Attribute *atts);

    // this combines two schemas, used as the result schema of a join
    Schema (const Schema& left, const Schema& right);

	// this constructs a sort order structure that can be used to
	// place a lexicographic ordering on the records using this type of schema
	int GetSortOrder (OrderMaker &order);
	
	void print(std::ostream& os = std::cout) const;

	~Schema ();

};

#endif
