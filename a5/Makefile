CC = g++ -O2 -w -Wno-deprecated

tag = -i

GOOGLETESTDIR = /home/yogesh/gtest
src = .
bin = .

ifdef linux
tag = -n
endif

googletest: Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Pipe.o y.tab.o lex.yy.o googletest.o
	$(CC) -o googletest Record.o Comparison.o ComparisonEngine.o Schema.o File.o  Pipe.o y.tab.o lex.yy.o googletest.o -I$(GOOGLETESTDIR)/include -L$(GOOGLETESTDIR)/lib -lgtest -lpthread

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o Pipe.o RelOp.o Function.o  Statistics.o QueryPlan.o Ddl.o Interpreter.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o Pipe.o RelOp.o Function.o  Statistics.o QueryPlan.o Ddl.o Interpreter.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o main.o -lfl -lpthread

test: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o Pipe.o RelOp.o Function.o  Statistics.o QueryPlan.o Ddl.o Interpreter.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o
	$(CC) -o test Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o Pipe.o RelOp.o Function.o  Statistics.o QueryPlan.o Ddl.o Interpreter.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o test.o -lfl -lpthread

googletest.o: googletest.cc
	$(CC) -o googletest.o -g -c -std=gnu++11 -I$(GOOGLETESTDIR)/include -L$(GOOGLETESTDIR)/lib -lgtest -lpthread googletest.cc

main.o: main.cc
	$(CC) -o main.o -g -c main.cc

test.o: test.cc
	$(CC) -o test.o -g -c test.cc

Interpreter.o: Interpreter.cc
	$(CC) -o Interpreter.o -g -c Interpreter.cc

Ddl.o: Ddl.cc
	$(CC) -o Ddl.o -g -c Ddl.cc

QueryPlan.o: QueryPlan.cc
	$(CC) -o QueryPlan.o -g -c QueryPlan.cc

Statistics.o: Statistics.cc
	$(CC) -o Statistics.o -g -c Statistics.cc
	
Comparison.o: Comparison.cc
	$(CC) -o Comparison.o -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -o ComparisonEngine.o -g -c ComparisonEngine.cc

Pipe.o: Pipe.cc
	$(CC) -o Pipe.o -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -o BigQ.o -g -c -std=gnu++11 BigQ.cc

SortedDBFile.o: SortedDBFile.cc
	$(CC) -o SortedDBFile.o -g -c SortedDBFile.cc

HeapDBFile.o: HeapDBFile.cc
	$(CC) -o HeapDBFile.o -g -c HeapDBFile.cc

GenericDBFile.o: GenericDBFile.cc
	$(CC) -o GenericDBFile.o -g -c GenericDBFile.cc

DBFile.o: DBFile.cc
	$(CC) -o DBFile.o -g -c DBFile.cc

RelOp.o: RelOp.cc
	$(CC) -o RelOp.o -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -o Function.o -g -c Function.cc

File.o: File.cc
	$(CC) -o File.o -g -c File.cc

Record.o: Record.cc
	$(CC) -o Record.o -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -o Schema.o -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	flex  Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	flex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean:
	rm -f *.exe 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*