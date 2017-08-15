using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.IO;

namespace MSCDataTool
{
    public partial class MSCDataTool : Form
    {

        public MSCDataTool()
        {
            InitializeComponent();
        }

        /// <summary>
        /// Open file
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void openToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Stream myStream;
            OpenFileDialog openFileDialog1 = new OpenFileDialog();

            openFileDialog1.InitialDirectory = "c:\\";
            openFileDialog1.Filter = "txt files (*.txt)|*.txt|All files (*.*)|*.*";
            openFileDialog1.FilterIndex = 2;
            openFileDialog1.RestoreDirectory = true;

            if (openFileDialog1.ShowDialog() == DialogResult.OK)
            {
                if ((myStream = openFileDialog1.OpenFile()) != null)
                {
                    // Insert code to read the stream here.
                    myStream.Close();
                }
            }
        }

        /// <summary>
        /// Save file (false = SaveDialog=no, true=yes
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void saveToolStripMenuItem_Click(object sender, EventArgs e)
        {
            SaveFile(false, "C:\\", "FileName", "txt");
        }

        /// <summary>
        /// New object
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void newToolStripMenuItem_Click(object sender, EventArgs e)
        {

        }

        /// <summary>
        /// Close Object
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void CloseToolStripMenuItem_Click(object sender, EventArgs e)
        {

        }

        /// <summary>
        /// Save as
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void saveAsToolStripMenuItem_Click(object sender, EventArgs e)
        {
            SaveFile(true, "C:\\", "FileName", "txt");
        }

        /// <summary>
        /// Exit Application
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void exitToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.Close();
        }

        /// <summary>
        /// Aboutbox
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void aboutToolStripMenuItem_Click(object sender, EventArgs e)
        {
            // Aboutbox form
            RuAbout ruAbout = new RuAbout();

            ruAbout.Show();
        }
        /// <summary>
        /// Save
        /// </summary>
        /// <param name="Modus false = SaveDialog=no, true=yes"></param>
        /// <param name="Directory"></param>
        /// <param name="FileName"></param>
        /// <param name="FileExtension"></param>
        void SaveFile(bool Modus, string Directory, string FileName, string FileExtension)
        {
            if (Modus == true)
            {
                SaveFileDialog saveFileDialog1 = new SaveFileDialog();
                saveFileDialog1.FileName = FileName;
                saveFileDialog1.DefaultExt = FileExtension;
                saveFileDialog1.InitialDirectory = Directory;
                saveFileDialog1.Filter = "txt files (*.txt)|*.txt|All files (*.*)|*.*";
                saveFileDialog1.FilterIndex = 2;
                saveFileDialog1.RestoreDirectory = true;

                if (saveFileDialog1.ShowDialog() == DialogResult.OK)
                {
                    /*
                    if ((myStream = saveFileDialog1.) != null)
                    {
                        // Insert code to read the stream here.
                        myStream.Close();
                    }
                     * */
                }
            }
            else
            {

            }
        }
    }
}
