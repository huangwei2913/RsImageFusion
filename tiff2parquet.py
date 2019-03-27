import os
import sys
import gdal
import rasterio
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def main(argv):

	gdal.AllRegister()
	ds = gdal.Open(argv[1])
	rowscount = ds.RasterYSize
	myarray_t = np.array(ds.GetRasterBand(1).ReadAsArray(), dtype=np.integer)
	
	cols = ds.RasterXSize
	rows = ds.RasterYSize
	print("x3 ndim: ", myarray_t.ndim)
	print("x3 shape:", myarray_t.shape)
	print("x3 size: ", myarray_t.size)	
	print("cols: ", cols)
	print("rows: ", rows)	
	#附加的一列
	blas = np.ones((rows,1))
	for i in range(rows):
		blas[i][0]=i
	
	
	myarray_t = np.hstack((myarray_t,blas))
	

	d = {}
	for i in range(cols+1):
		row = myarray_t[:,i]
		d[str(i)] = row
		
	df = pd.DataFrame(d)
	table = pa.Table.from_pandas(df)
	pq.write_table(table, argv[1]+'.parquet', compression='GZIP', use_dictionary=False)

if __name__ == '__main__':
	main(sys.argv)




	









