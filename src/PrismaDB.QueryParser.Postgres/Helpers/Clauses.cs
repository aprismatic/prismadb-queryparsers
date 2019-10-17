using PrismaDB.QueryAST.DML;
using PrismaDB.QueryAST.Result;
using System;
using System.Collections.Generic;

namespace PrismaDB.QueryParser.Postgres
{
    internal class AndClause : Expression
    {
        public Expression left, right;

        public AndClause(Expression left, Expression right)
        {
            this.left = left;
            this.right = right;
        }

        public override object Clone() => new AndClause((Expression)left.Clone(), (Expression)right.Clone());

        public override bool Equals(object other) => throw new NotImplementedException();
        public override object Eval(ResultRow r) => throw new NotImplementedException();
        public override List<ColumnRef> GetColumns() => throw new NotImplementedException();
        public override int GetHashCode() => throw new NotImplementedException();
        public override bool UpdateChild(Expression child, Expression newChild) => throw new NotImplementedException();
        public override List<ConstantContainer> GetConstants() => throw new NotImplementedException();
        public override string ToString() => throw new NotImplementedException();
    }

    internal class OrClause : Expression
    {
        public Expression left, right;

        public OrClause(Expression left, Expression right)
        {
            this.left = left;
            this.right = right;
        }

        public override object Clone() => new OrClause((Expression)left.Clone(), (Expression)right.Clone());

        public override bool Equals(object other) => throw new NotImplementedException();
        public override object Eval(ResultRow r) => throw new NotImplementedException();
        public override List<ColumnRef> GetColumns() => throw new NotImplementedException();
        public override int GetHashCode() => throw new NotImplementedException();
        public override bool UpdateChild(Expression child, Expression newChild) => throw new NotImplementedException();
        public override List<ConstantContainer> GetConstants() => throw new NotImplementedException();
        public override string ToString() => throw new NotImplementedException();
    }
}
