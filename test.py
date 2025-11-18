#from glob import glob
import pandas as df
import pymupdf

#files = glob('Z:\Alpha Student Beneficiaries)

with pymupdf.open("./Alpha Student Beneficiaries @ 06 November 2025.pdf") as f:
    with open("beneficiary_list.txt","wb") as out: # create a text outpout
        for page in f: # Iterate the document pages
            text = page.get_text().encode("utf8")
            out.write(text)
            out.write(bytes((12,))) # write page delimiter (form feed 0x0C)

print("Done printing to file")
