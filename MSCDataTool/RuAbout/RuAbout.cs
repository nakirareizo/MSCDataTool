using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MSCDataTool
{
    public partial class RuAbout : Form
    {
        public RuAbout()
        {
            InitializeComponent();
            AssemblyInfo assemblyInfo = new AssemblyInfo();

            this.textBoxName.Text = assemblyInfo.AsmName;
            this.textBoxVersion.Text = assemblyInfo.Version.ToString();
            this.textBoxCopyright.Text = assemblyInfo.Copyright;
        }

        private void RuAbout_Click(object sender, EventArgs e)
        {
            this.Close();
        }


    }
}
