using System;
using System.Collections.Generic;
using PrismaDB.QueryAST.DML;
using PrismaDB.QueryAST.Result;

namespace PrismaDB.QueryParser.MSSQL
{
    /// <summary>
    ///     Additional AndClause to assist with conversion to CNF form.
    ///     Not available in QueryAST.
    /// </summary>
    internal class AndClause : Expression
    {
        public Expression left, right;

        public AndClause(Expression left, Expression right)
        {
            this.left = left;
            this.right = right;
        }

        public override object Clone()
        {
            var left_clone = left.Clone() as Expression;
            var right_clone = right.Clone() as Expression;

            var clone = new AndClause(left_clone, right_clone);

            return clone;
        }

        public override object Eval(ResultRow r)
        {
            throw new NotImplementedException();
        }

        public override List<ColumnRef> GetColumns()
        {
            throw new NotImplementedException();
        }

        public override void setValue(params object[] value)
        {
            throw new NotImplementedException();
        }

        public override string ToString()
        {
            throw new NotImplementedException();
        }

        public override bool Equals(object other)
        {
            throw new NotImplementedException();
        }

        public override int GetHashCode()
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    ///     Additional OrClause to assist with conversion to CNF form.
    ///     Not available in QueryAST.
    /// </summary>
    internal class OrClause : Expression
    {
        public Expression left, right;

        public OrClause(Expression left, Expression right)
        {
            this.left = left;
            this.right = right;
        }

        public override object Clone()
        {
            var left_clone = left.Clone() as Expression;
            var right_clone = right.Clone() as Expression;

            var clone = new OrClause(left_clone, right_clone);

            return clone;
        }

        public override object Eval(ResultRow r)
        {
            throw new NotImplementedException();
        }

        public override List<ColumnRef> GetColumns()
        {
            throw new NotImplementedException();
        }

        public override void setValue(params object[] value)
        {
            throw new NotImplementedException();
        }

        public override string ToString()
        {
            throw new NotImplementedException();
        }

        public override bool Equals(object other)
        {
            throw new NotImplementedException();
        }

        public override int GetHashCode()
        {
            throw new NotImplementedException();
        }
    }
}