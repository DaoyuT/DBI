#include "Statistics.h"
#include "Errors.h"
#include <climits>
#include <stdio.h>
#include <cstring>
#include <iostream>
#include <stdlib.h> 

Statistics::Statistics(){}

Statistics::Statistics(Statistics &copyMe)
{
	for (map<string,relSchema>::iterator it = copyMe.relMap.begin(); it != copyMe.relMap.end(); ++it){
		string relN = it->first;
		relSchema rS;
		rS.numTuples = it->second.numTuples;
		rS.numRel = it->second.numRel;
		
		for (map<string, int>::iterator it_a = it->second.attMap.begin(); it_a!=it->second.attMap.end(); ++it_a){
			string attN = it_a->first;
			int n = it_a->second;
			rS.attMap.insert(pair<string, int>(attN, n));
		}
		relMap.insert(pair<string, relSchema>(relN, rS));
		rS.attMap.clear();
	}
}

Statistics::~Statistics()
{
	for (map<string,relSchema>::iterator it = relMap.begin(); it != relMap.end(); ++it){
		it->second.attMap.clear();
	}
	relMap.clear();
}

char* Statistics::SearchAttr(char * attrName){
	map<string, int>::iterator it;
	string attrStr(attrName);
	string rel;
	char * relName = new char[200];
	for (map<string, relSchema>::iterator it1 = relMap.begin(); it1 != relMap.end(); ++it1){
		it = it1->second.attMap.find(attrStr);
		if (it != it1->second.attMap.end()){
			rel = it1->first;
			break;
		}
	}
	return (char*)rel.c_str();
}

void Statistics::AddRel(char *relName, int numTuples)
{	
	string relN(relName);
	//if relName not in relMap, insert a new relSchema
	map<string,relSchema>::iterator it = relMap.find(relN);
	if(it == relMap.end()){
		relSchema rS;
		rS.numRel = 1;
		rS.numTuples = numTuples;
		relMap.insert(pair<string, relSchema>(relN,rS));
	}
	//if relName in relMap, just simply update numTuples
	else{
		it->second.numTuples = numTuples;
	}
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
	//cout<<"\n"<<relName<<" : "<<attName;

	string relN(relName);
	string attN(attName);
	map<string,relSchema>::iterator it = relMap.find(relN);
	if(it == relMap.end()){
		cerr << "Error: relName not found when adding a attribute!";
		return;
	}
	map<string, int>::iterator it_a = it->second.attMap.find(attN);
	//if attName not in attMap, insert a new attribute
	if(it_a == it->second.attMap.end()){
		if (numDistincts == -1)
			numDistincts = it->second.numTuples;
		it->second.attMap.insert(pair<string, int>(attN, numDistincts));
	}
	//if attName in attMap, just simply update numDistincts
	else{
		if (numDistincts == -1)
			numDistincts = it->second.numTuples;
		it_a->second = numDistincts;
	}
}

void Statistics::CopyRel(char *oldName, char *newName)
{
	cout<<"\n"<<oldName<<" : "<<newName;
	
	string oldN(oldName);
	string newN(newName);
	map<string,relSchema>::iterator it = relMap.find(oldN);
	if(it == relMap.end()){
		cerr << "Error: relName not found when adding a attribute!";
		return;
	}
	//if new name already exist, return
	if(relMap.find(newN) != relMap.end()){
		return;
	}
	//start copying
	relSchema rS;
	rS.numTuples = it->second.numTuples;
	rS.numRel = it->second.numRel;
	for (map<string, int>::iterator it_a = it->second.attMap.begin(); it_a != it->second.attMap.end(); ++it_a){
		char * newAtt = new char[100];
		sprintf(newAtt, "%s.%s", newName, it_a->first.c_str());
		string temp(newAtt);
		rS.attMap.insert(pair<string,int>(temp ,it_a->second));
		delete newAtt;
	}
	relMap.insert(pair<string,relSchema>(newN,rS));
}
	
void Statistics::Read(char *fromWhere)
{
	relMap.clear();
	FILE* readfile;
	readfile = fopen(fromWhere, "r");
	if(readfile == NULL){
		readfile = fopen(fromWhere, "w");
		fprintf(readfile, "end\n");
		fclose(readfile);
		return;
	}
	char fstr[100], relchar[100];
	int n;
	fscanf(readfile, "%s", fstr);
	while(strcmp(fstr, "end")){
		if(!strcmp(fstr, "rel")){
			relSchema rS;
			rS.numRel = 0;
			fscanf(readfile, "%s", fstr);
			string relstr(fstr);
			strcpy(relchar, relstr.c_str());
			fscanf(readfile, "%s", fstr);
			rS.numTuples = atoi(fstr);
			fscanf(readfile, "%s", fstr);
			//"attrs"
			fscanf(readfile, "%s", fstr);
			
			while(strcmp(fstr, "rel") && strcmp(fstr, "end")){
				string attstr(fstr);				
				fscanf(readfile, "%s", fstr);
				n = atoi(fstr);
				rS.attMap.insert(pair<string, int>(attstr, n));
				fscanf(readfile, "%s", fstr);
			}
			relMap.insert(pair<string, relSchema>(relstr, rS));			
			rS.numRel++;	
		}
	}
	fclose(readfile);
}

void Statistics::Write(char *toWhere)
{
	FILE* writefile;
	writefile = fopen(toWhere, "w");
	for(map<string,relSchema>::iterator rit = relMap.begin(); rit != relMap.end(); ++rit){
		char * rbuffer = new char[rit->first.length()+1];
		strcpy(rbuffer, rit->first.c_str());
		fprintf(writefile, "rel\n%s\n", rbuffer);
		fprintf(writefile, "%d\nattrs\n", rit->second.numTuples);
		for(map<string,int>::iterator ait = rit->second.attMap.begin(); ait != rit->second.attMap.end(); ++ait){
			char * abuffer = new char[ait->first.length()+1];
			strcpy(abuffer, ait->first.c_str());
			fprintf(writefile, "%s\n%d\n", abuffer, ait->second);
			delete abuffer;
		}
		delete rbuffer;
	}
	fprintf(writefile, "end\n");
	fclose(writefile);
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
	Estimate(parseTree, relNames, numToJoin);
	struct AndList * andlist = parseTree;
	struct OrList * orlist;
	//if andlist == NULL TBc
	while (andlist != NULL){
		if (andlist->left != NULL){
			orlist = andlist->left;
			while(orlist != NULL){
				//if join
				if (orlist->left->left->code == NAME && orlist->left->right->code == NAME){
					map<string, int>::iterator itAtt[2];
					map<string, relSchema>::iterator itRel[2];
					string joinAtt1(orlist->left->left->value);
					string joinAtt2(orlist->left->right->value);
					for (map<string, relSchema>::iterator rit = relMap.begin(); rit != relMap.end(); ++rit){
						itAtt[0] = rit->second.attMap.find(joinAtt1);
						//if found
						//add name constrains TBC
						if(itAtt[0] != rit->second.attMap.end()){
							itRel[0] = rit;
							break;
						}
					}
					for (map<string, relSchema>::iterator rit = relMap.begin(); rit != relMap.end(); ++rit){
						itAtt[1] = rit->second.attMap.find(joinAtt2);
						//if found
						//add name constrains TBC
						if(itAtt[1] != rit->second.attMap.end()){
							itRel[1] = rit;
							break;
						}
					}
					//creating a new relation schema
					relSchema joinedRel;
					char * joinName = new char[200];
					sprintf(joinName, "%s|%s", itRel[0]->first.c_str(), itRel[1]->first.c_str());
					string joinNamestr(joinName);
					joinedRel.numTuples = tempRes;
					joinedRel.numRel = numToJoin;
					for(int i = 0; i < 2; i++){
						for (map<string, int>::iterator ait = itRel[i]->second.attMap.begin(); ait != itRel[i]->second.attMap.end(); ++ait){
							joinedRel.attMap.insert(*ait);
						}
						//!!!
						relMap.erase(itRel[i]);
					}
					relMap.insert(pair<string, relSchema>(joinNamestr, joinedRel));
				}
				//if select
				else{
					string seleAtt(orlist->left->left->value);
					map<string, int>::iterator itAtt;
					map<string, relSchema>::iterator itRel;
					for (map<string, relSchema>::iterator rit = relMap.begin(); rit != relMap.end(); ++rit){
						itAtt = rit->second.attMap.find(seleAtt);
						if(itAtt != rit->second.attMap.end()){
							itRel = rit;
							break;
						}
					}
					//store the temp result
					itRel->second.numTuples = tempRes;					
				}
				//to next
				orlist = orlist->rightOr;
			}
		}
		//to next
		andlist = andlist->rightAnd;
	}
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
	struct AndList * andlist = parseTree;
	struct OrList * orlist;
	double result = 0.0, fraction = 1.0;
	int state = 0; // most left? 0 : 1
	//if andlist == NULL TBC
	while (andlist != NULL){
		if (andlist->left != NULL){
			orlist = andlist->left;
			double fractionOr = 0.0;// fraction within the orlist
			map<string, int>::iterator lastAtt;
			while(orlist != NULL){
				//if join
				if (orlist->left->left->code == NAME && orlist->left->right->code == NAME){
					map<string, int>::iterator itAtt[2];
					map<string, relSchema>::iterator itRel[2];
					string joinAtt1(orlist->left->left->value);
					string joinAtt2(orlist->left->right->value);

					for (map<string, relSchema>::iterator rit = relMap.begin(); rit!=relMap.end(); ++rit){
						itAtt[0] = rit->second.attMap.find(joinAtt1);
						if(itAtt[0] != rit->second.attMap.end()){
							itRel[0] = rit;
							break;
						}
					}
					for (map<string, relSchema>::iterator rit = relMap.begin(); rit!=relMap.end(); ++rit){
						itAtt[1] = rit->second.attMap.find(joinAtt2);
						if(itAtt[1] != rit->second.attMap.end()){
							itRel[1] = rit;
							break;
						}
					}
					
					double max;
					if (itAtt[0]->second >= itAtt[1]->second)		
						max = (double)itAtt[0]->second;
					else		
						max = (double)itAtt[1]->second;
					if (state == 0)
						result = (double)itRel[0]->second.numTuples*(double)itRel[1]->second.numTuples/max;
					else
						result = result*(double)itRel[1]->second.numTuples/max;

					state = 1;
				}
				else{
					string seleAtt(orlist->left->left->value);
					map<string, int>::iterator itAtt;
					map<string, relSchema>::iterator itRel;
					for (map<string, relSchema>::iterator rit = relMap.begin(); rit!=relMap.end(); ++rit){
						itAtt = rit->second.attMap.find(seleAtt);
						if(itAtt != rit->second.attMap.end()){
							itRel = rit;
							break;
						}
					}
					if (result == 0.0)
						result = ((double)itRel->second.numTuples);
					double tempFrac;
					if(orlist->left->code == EQUALS)
						tempFrac = 1.0 / itAtt->second;
					else
						tempFrac = 1.0 / 3.0;
					if(lastAtt != itAtt)
						fractionOr = tempFrac+fractionOr-(tempFrac*fractionOr);
					else
						fractionOr += tempFrac;
					//cout << "fractionOr: " << fractionOr << endl;
					lastAtt = itAtt;//
				}
				orlist = orlist->rightOr;
			}
			if (fractionOr != 0.0)
				fraction = fraction*fractionOr;
			//cout << "fraction: " << fraction << endl;
		}
		andlist = andlist->rightAnd;
	}
	result = result * fraction;
	//cout << "result: " << result << endl;
	tempRes = result;
	return result;
}

int Statistics::getNumTuplesInRelation(string relName){
	return relMap.find(relName)->second.numTuples;
}