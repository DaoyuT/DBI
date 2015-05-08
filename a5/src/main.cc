
#include <iostream>
#include <stdlib.h>
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryPlan.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

char* catalog_path = "catalog";
char* dbfile_dir = "../dbfile/";
char* tpch_dir = "../data/";

// from parser
extern FuncOperator* finalFunction;
extern TableList* tables;
extern AndList* boolean;
extern NameList* groupingAtts;
extern NameList* attsToSelect;
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

void PrintParseTree(struct AndList *pAnd)
{
  cout << "(";
  while (pAnd)
    {
      struct OrList *pOr = pAnd->left;
      while (pOr)
        {
          struct ComparisonOp *pCom = pOr->left;
          if (pCom!=NULL)
            {
              {
                struct Operand *pOperand = pCom->left;
                if(pOperand!=NULL)
                  {
                    cout<<pOperand->value<<"";
                  }
              }
              switch(pCom->code)
                {
                case LESS_THAN:
                  cout<<" < "; break;
                case GREATER_THAN:
                  cout<<" > "; break;
                case EQUALS:
                  cout<<" = "; break;
                default:
                  cout << " unknown code " << pCom->code;
                }
              {
                struct Operand *pOperand = pCom->right;
                if(pOperand!=NULL)
                  {
                    cout<<pOperand->value<<"";
                  }
              }
            }
          if(pOr->rightOr)
            {
              cout<<" OR ";
            }
          pOr = pOr->rightOr;
        }
      if(pAnd->rightAnd)
        {
          cout<<") AND (";
        }
      pAnd = pAnd->rightAnd;
    }
  cout << ")" << endl;
}

void PrintTablesAliases (TableList * tblst)
{
  while (0 != tblst)
    {
      cout << "Table " << tblst->tableName << " is aliased to " << tblst->aliasAs << endl;
      tblst = tblst->next;
    }
}

void PrintNameList(NameList * nmlst)
{
  while (0 != nmlst)
    {
      cout << nmlst->name << "&";
      nmlst = nmlst->next;
    }
  cout << endl;
}
int main () {

	cout << "\n enter CNF predicate (when done press ctrl-D):\n\t";
  	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF.\n";
		exit (1);
	}
	cout<<"distinctAtts: "<<distinctAtts<<endl;
	cout<<"distinctFunc: "<<distinctFunc<<endl;
	cout<<"Printing tables"<<endl;
	PrintTablesAliases(tables);
	cout<<"Printing groupingAtts"<<endl;
	PrintNameList(groupingAtts);
	cout<<"Printing attsToSelect"<<endl;
	PrintNameList(attsToSelect);
	cout<<"Printing AndList"<<endl;
	PrintParseTree(boolean);

	
  Statistics stats;
  stats.Read("Statistics.txt");
  
  QueryPlan qp(&stats);
  qp.plan();
  qp.print();
  qp.execute();

  /*
  TableList *traverse= tables;
  vector<string> rels;
  cout<<"Table Names = "<<endl;
  while(traverse){
    string str(traverse->tableName);
    rels.push_back(str);
    cout<<"  "<<str<<endl;

    if(traverse->aliasAs != NULL)
      stats.CopyRel(traverse->tableName, traverse->aliasAs);

    traverse = traverse->next;
  }
  
  //create attributes
  NameList *temp = attsToSelect;
  vector<string> atts;
  cout<<"Attribute Names = "<<endl;
  while (temp) {
    string s(temp->name);
    atts.push_back(s);
    cout<<"::  "<<s<<endl;
    temp = temp->next;
  }  
  
  QueryTreeNode *t;
  QueryTreeNode * head = NULL;
  QueryTreeNode *runningHead = NULL;
  QueryPlan ee;
    
  /*cout<<"Grouping done ..."<<endl;
  //selection list
  NameList * tempSelect = attsToSelect;
  int selCount = 0; // number of attributes in the selection List
  vector<string> vNameList;
  while(tempSelect){
    selCount ++;
    vNameList.push_back(tempSelect->name);
    tempSelect = tempSelect->next;
  }*/
  
  //v: tableNames, v1: selectAttrNames
  /*t = ee.enumerate(rels, atts, boolean, stats, 0);
  t->print();
  */
}


