using PrismaDB.QueryAST;
using PrismaDB.QueryParser;
using System;
using System.Collections.Generic;
using System.Windows.Forms;

namespace SqlParsingDemo
{
    public partial class Form1 : Form
    {
        SqlParser parser = new SqlParser();

        public Form1()
        {
            InitializeComponent();
        }

        private void btnParse_Click(object sender, EventArgs e)
        {
            txtOutput.Text = "";
            List<Query> queries = parser.ParseToAST(txtSource.Text);
            foreach (Query q in queries)
            {
                txtOutput.Text += q.ToString() + Environment.NewLine;
            }
        }
    }
}
