using PrismaDB.QueryAST;
using PrismaDB.QueryParser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test_Project
{
    class Program
    {

        static void Main(string[] args)
        {
            SqlParser parser;
            parser = new SqlParser();

            string test = "CREATE TABLE ttt (aaa INT ENCRYPTED FOR (INTEGER_ADDITION, integer_multiplication), bbb VARCHAR(80))";
            List<Query> queries = parser.ParseToAST(test);

            foreach(Query q in queries)
            {
               Console.WriteLine(q.ToString());
            }
        }
    }
}
