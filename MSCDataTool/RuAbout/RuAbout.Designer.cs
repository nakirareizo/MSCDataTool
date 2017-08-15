namespace MSCDataTool
{
    partial class RuAbout
    {
        /// <summary> 
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary> 
        /// Required method for Designer support - do not modify 
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(RuAbout));
            this.buttonOk = new System.Windows.Forms.Button();
            this.pictureBoxKR = new System.Windows.Forms.PictureBox();
            this.labelAppName = new System.Windows.Forms.Label();
            this.labelVersion = new System.Windows.Forms.Label();
            this.labelCopyright = new System.Windows.Forms.Label();
            this.textBoxName = new System.Windows.Forms.TextBox();
            this.textBoxVersion = new System.Windows.Forms.TextBox();
            this.textBoxCopyright = new System.Windows.Forms.TextBox();
            ((System.ComponentModel.ISupportInitialize)(this.pictureBoxKR)).BeginInit();
            this.SuspendLayout();
            // 
            // buttonOk
            // 
            this.buttonOk.DialogResult = System.Windows.Forms.DialogResult.Cancel;
            this.buttonOk.Location = new System.Drawing.Point(236, 104);
            this.buttonOk.Name = "buttonOk";
            this.buttonOk.Size = new System.Drawing.Size(48, 24);
            this.buttonOk.TabIndex = 5;
            this.buttonOk.Text = "Ok";
            // 
            // pictureBoxKR
            // 
            this.pictureBoxKR.Image = ((System.Drawing.Image)(resources.GetObject("pictureBoxKR.Image")));
            this.pictureBoxKR.Location = new System.Drawing.Point(4, 5);
            this.pictureBoxKR.Name = "pictureBoxKR";
            this.pictureBoxKR.Size = new System.Drawing.Size(48, 32);
            this.pictureBoxKR.TabIndex = 6;
            this.pictureBoxKR.TabStop = false;
            this.pictureBoxKR.Click += new System.EventHandler(this.RuAbout_Click);
            // 
            // labelAppName
            // 
            this.labelAppName.Font = new System.Drawing.Font("Microsoft Sans Serif", 10F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.labelAppName.Location = new System.Drawing.Point(60, 13);
            this.labelAppName.Name = "labelAppName";
            this.labelAppName.Size = new System.Drawing.Size(83, 24);
            this.labelAppName.TabIndex = 7;
            this.labelAppName.Text = "AppName";
            this.labelAppName.Click += new System.EventHandler(this.RuAbout_Click);
            // 
            // labelVersion
            // 
            this.labelVersion.Font = new System.Drawing.Font("Microsoft Sans Serif", 9F, ((System.Drawing.FontStyle)((System.Drawing.FontStyle.Bold | System.Drawing.FontStyle.Italic))), System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.labelVersion.Location = new System.Drawing.Point(60, 45);
            this.labelVersion.Name = "labelVersion";
            this.labelVersion.Size = new System.Drawing.Size(83, 24);
            this.labelVersion.TabIndex = 8;
            this.labelVersion.Text = "Version";
            this.labelVersion.Click += new System.EventHandler(this.RuAbout_Click);
            // 
            // labelCopyright
            // 
            this.labelCopyright.Font = new System.Drawing.Font("Microsoft Sans Serif", 8.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.labelCopyright.Location = new System.Drawing.Point(60, 77);
            this.labelCopyright.Name = "labelCopyright";
            this.labelCopyright.Size = new System.Drawing.Size(83, 24);
            this.labelCopyright.TabIndex = 9;
            this.labelCopyright.Text = "Copyright";
            this.labelCopyright.Click += new System.EventHandler(this.RuAbout_Click);
            // 
            // textBoxName
            // 
            this.textBoxName.BackColor = System.Drawing.SystemColors.Control;
            this.textBoxName.Location = new System.Drawing.Point(149, 13);
            this.textBoxName.Name = "textBoxName";
            this.textBoxName.Size = new System.Drawing.Size(156, 20);
            this.textBoxName.TabIndex = 10;
            this.textBoxName.Click += new System.EventHandler(this.RuAbout_Click);
            // 
            // textBoxVersion
            // 
            this.textBoxVersion.BackColor = System.Drawing.SystemColors.Control;
            this.textBoxVersion.Location = new System.Drawing.Point(149, 44);
            this.textBoxVersion.Name = "textBoxVersion";
            this.textBoxVersion.Size = new System.Drawing.Size(156, 20);
            this.textBoxVersion.TabIndex = 11;
            this.textBoxVersion.Click += new System.EventHandler(this.RuAbout_Click);
            // 
            // textBoxCopyright
            // 
            this.textBoxCopyright.BackColor = System.Drawing.SystemColors.Control;
            this.textBoxCopyright.Location = new System.Drawing.Point(149, 74);
            this.textBoxCopyright.Name = "textBoxCopyright";
            this.textBoxCopyright.Size = new System.Drawing.Size(156, 20);
            this.textBoxCopyright.TabIndex = 12;
            this.textBoxCopyright.Click += new System.EventHandler(this.RuAbout_Click);
            // 
            // RuAbout
            // 
            this.AcceptButton = this.buttonOk;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(317, 100);
            this.Controls.Add(this.textBoxCopyright);
            this.Controls.Add(this.textBoxVersion);
            this.Controls.Add(this.textBoxName);
            this.Controls.Add(this.buttonOk);
            this.Controls.Add(this.pictureBoxKR);
            this.Controls.Add(this.labelAppName);
            this.Controls.Add(this.labelVersion);
            this.Controls.Add(this.labelCopyright);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "RuAbout";
            this.ShowIcon = false;
            this.ShowInTaskbar = false;
            this.Text = "Info";
            this.Click += new System.EventHandler(this.RuAbout_Click);
            ((System.ComponentModel.ISupportInitialize)(this.pictureBoxKR)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button buttonOk;
        private System.Windows.Forms.PictureBox pictureBoxKR;
        private System.Windows.Forms.Label labelAppName;
        private System.Windows.Forms.Label labelVersion;
        private System.Windows.Forms.Label labelCopyright;
        private System.Windows.Forms.TextBox textBoxName;
        private System.Windows.Forms.TextBox textBoxVersion;
        private System.Windows.Forms.TextBox textBoxCopyright;
    }
}
