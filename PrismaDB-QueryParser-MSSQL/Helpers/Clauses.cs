using System;
using System.Collections.Generic;
using PrismaDB.QueryAST.DML;
using PrismaDB.QueryAST.Result;

namespace PrismaDB.QueryParser.MySQL
{
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
            return new AndClause((Expression)left.Clone(), (Expression)right.Clone());
        }

        public override bool Equals(object other) { throw new NotImplementedException(); }
        public override object Eval(ResultRow r) { throw new NotImplementedException(); }
        public override List<ColumnRef> GetColumns() { throw new NotImplementedException(); }
        public override int GetHashCode() { throw new NotImplementedException(); }
        public override List<ColumnRef> GetNoCopyColumns() { throw new NotImplementedException(); }
        public override string ToString() { throw new NotImplementedException(); }
    }

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
            return new OrClause((Expression)left.Clone(), (Expression)right.Clone());
        }

        public override bool Equals(object other) { throw new NotImplementedException(); }
        public override object Eval(ResultRow r) { throw new NotImplementedException(); }
        public override List<ColumnRef> GetColumns() { throw new NotImplementedException(); }
        public override int GetHashCode() { throw new NotImplementedException(); }
        public override List<ColumnRef> GetNoCopyColumns() { throw new NotImplementedException(); }
        public override string ToString() { throw new NotImplementedException(); }
    }
}
