# 3D Coronavirus SARS-CoV-2 Protein Visualization

In this project you will create an interactive three-dimensional (3D) representation of SARS-CoV-19 (Coronavirus) protein structures & publication-quality pictures of the same, understand properties of SARS-CoV-19 genome, handle biological sequence data stored in FASTA & PDB (Protein Data Bank), and get insights from this data using Biopython. This hands-on notebook will also give you a glimpse of tasks a Bioinformatician performs on a daily basis, along with the up-to-date concepts and database use cases in the field of Medical Research and Human genetics. In this notebook, we will also cover basics about important databases used by biologists and biotechnologists, along with the type of sequence data we can access and visualize from these databases using [Biopython](https://biopython.org) & [nglview](https://github.com/nglviewer/nglview).



## Objectives

* Biological sequence and data manipulation with Biopython.

* Create an interactive 3D model of SARS-CoV-19 Protein.

* Query Biological Databases using Biopython & web-scraping.




## Table of contents

1. Introduction to Biopython. Brief introduction to Biopython, its applications and listing the modules/attributes which are built into Biopython.    

2. Understand FASTA file format. Reading and parsing biological sequences stored in FASTA format, describe, and calculate length and molecular weight of the sequence.    

3. Sequence manipulation using Biopython. Applying basic python indexing techniques to fetch a specific section of the biological sequence, compute GC content and search for codons and complement sequences using Biopython modules.

4. Transcription & Translation studies. Brief introduction to transcription and translation process, learn to transcribe and translate the sequence data, visualize amino acid content, and store sequences in Pandas Data frame.

5. Perform Basic Local Alignment using NCBI-BLAST. Performing BLAST (Basic Local Alignment Search Tool) on translated amino acid sequence using BLAST tool from NCBI (National Center for Biotechnology Information) Database and list the parameters to identify the best aligned sequence.

6. Reading PDB file. Retrieving PDB files from Protein Data Bank database using PDB ID from previous task, read and parse PDB files and fetch features of protein sequence.

7. Visualizing SARS-CoV-19 Protein structures using nglview. Creating an interactive visualization of protein, modify the structure to fit your preference and identify features of SARS-CoV-19 protein.



## Directory layout

    .
    ├── 3D_Protein_Vis.ipynb             # Example Jupyter notebook
    ├── 3D_Protein_Vis.html              # Example Jupyter notebook in HTML
    ├── 3D_Protein_Vis_demo_view.html    # Simple demo for nglview interactive visualization
    ├── 3D_Protein_Vis_7D4F_view.html    # Interactive visualization of PDB sequence 7D4F
    ├── 3D_Protein_Vis_A_view.html       # Interactive visualization of PDB sequence 7D4F chain A
    ├── 3D_Protein_Vis_B_view.html       # Interactive visualization of PDB sequence 7D4F chain B
    ├── 3D_Protein_Vis_7D4F_gui.png      # nglview GUI for PDB sequence 7D4F
    └── readme.md                        # Current file