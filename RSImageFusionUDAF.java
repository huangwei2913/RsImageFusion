package org.apache.spark.examples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.Deflater;



public class RSWeigthUDAF extends UserDefinedAggregateFunction
{

	private StructType inputSchema;		//表示输入会接收多少列数据
    private StructType bufferSchema;
	private DataType returnDataType =  DataTypes.BinaryType ; //返回字符串结果，形式例如(0_0):[0_1,0_2] 
	private int baselineRowNumber=0;	//初始化时是一个负数，每当新的行加入时，自动更新这个值,代表窗口中的当前行的值			
	private Double thread1V=0.0d;  		//fine1相似像元判断
	private Double thread2V=0.0d;  		//fine2相似像元判断
	private int windowSize=1;
	
	MutableAggregationBuffer magbbuffer;
	
	
	public RSImageFusionUDAF(Double thread1V,Double thread2V,int windowSize){
	
		thread1V = thread1V;
		thread2V = thread2V;
		windowSize = windowSize;
		//定义输入列数为所有列
		List<StructField> inputFields = new ArrayList<StructField>();
		StructField ifield0 = DataTypes.createStructField("rowKey",DataTypes.IntegerType,true);
		StructField ifield1 = DataTypes.createStructField("value1", DataTypes.BinaryType,true);
		StructField ifield2= DataTypes.createStructField("value2", DataTypes.BinaryType,true);
		StructField ifield3 = DataTypes.createStructField("value3", DataTypes.BinaryType,true);
		StructField ifield4 = DataTypes.createStructField("value4", DataTypes.BinaryType,true);
		StructField ifield5 = DataTypes.createStructField("BBB", DataTypes.BinaryType,true);
		
		inputFields.add(ifield0);
		inputFields.add(ifield1);
		inputFields.add(ifield2);
		inputFields.add(ifield3);
		inputFields.add(ifield4);
		inputFields.add(ifield5);
		inputSchema = DataTypes.createStructType(inputFields);
		
		//定义缓冲区数据存储类型,由于都要进行5窗口卷积
		List<StructField> bufferFields = new ArrayList<StructField>();
		StructField bfield0 = DataTypes.createStructField("rowKey",DataTypes.StringType,true); //存储每一行行号
		StructField bfield1 = DataTypes.createStructField("fine1",DataTypes.BinaryType, true);	
		StructField bfield2 = DataTypes.createStructField("fine2",DataTypes.BinaryType, true);	
		StructField bfield3 = DataTypes.createStructField("coarse1",DataTypes.BinaryType, true);	
		StructField bfield4 = DataTypes.createStructField("coarse2",DataTypes.BinaryType, true);
		StructField bfield5 = DataTypes.createStructField("coarse0",DataTypes.BinaryType, true);

		bufferFields.add(bfield0);
		bufferFields.add(bfield1);
		bufferFields.add(bfield2);
		bufferFields.add(bfield3);
		bufferFields.add(bfield4);
		bufferFields.add(bfield5);
		//定义bufferSchema的数据类型
		bufferSchema = DataTypes.createStructType(bufferFields);	
		
	}
	
	public  byte[] compress(byte[] data) throws IOException {  
		Deflater deflater = new Deflater();  
		deflater.setInput(data);  
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);   
		deflater.finish();  
		byte[] buffer = new byte[1024];   
		while (!deflater.finished()) {  
		int count = deflater.deflate(buffer); // returns the generated code... index  
		outputStream.write(buffer, 0, count);   
		}  
		outputStream.close();  
		byte[] output = outputStream.toByteArray();  
		return output;  
	}  
	
	
	// Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
	// standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
	// the opportunity to update its values. Note that arrays and maps inside the buffer are still
	// immutable. 
	@Override
    public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0,"");
		buffer.update(1,null);
		buffer.update(2,null);
		buffer.update(3,null);
		buffer.update(4,null);
		buffer.update(5,null);		
		magbbuffer = buffer;

    }
		
	
	

	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		
			buffer.update(0, buffer.get(0).toString()+","+input.get(0).toString()); 		//store row keys
			
			byte[] inputObj1 = (byte[])input.get(1);	
			byte[] fine1= Arrays.copyOfRange(inputObj1,0,inputObj1.length);

			byte[] inputObj2 = (byte[])input.get(2);	
			byte[] fine2= Arrays.copyOfRange(inputObj2,0,inputObj2.length);	
			
			byte[] inputObj3 = (byte[])input.get(3);	
			byte[] coarse1= Arrays.copyOfRange(inputObj3,0,inputObj3.length);	
			
			byte[] inputObj4 = (byte[])input.get(4);	
			byte[] coarse2= Arrays.copyOfRange(inputObj4,0,inputObj4.length);
			
			byte[] inputObj5 = (byte[])input.get(5);	
			byte[] coarse0= Arrays.copyOfRange(inputObj4,0,inputObj4.length);			
			
			if(buffer.get(1)==null){
				buffer.update(1,Arrays.copyOfRange(fine1,0,fine1.length));
			}else{
				byte[] orginalObj = (byte[])buffer.get(1);	
				byte[] tmporiginal= Arrays.copyOfRange(orginalObj,0,orginalObj.length); 	
				byte[] allDatas = new byte[tmporiginal.length+ fine1.length];
				System.arraycopy(tmporiginal,0,allDatas,0,tmporiginal.length);
				System.arraycopy(fine1,0,allDatas,tmporiginal.length,fine1.length);
				buffer.update(1,Arrays.copyOfRange(allDatas,0,allDatas.length));	
			}
			
						
			if(buffer.get(2)==null){
				buffer.update(2,Arrays.copyOfRange(fine2,0,fine2.length));
			}else{
				byte[] orginalObj = (byte[])buffer.get(2);	
				byte[] tmporiginal= Arrays.copyOfRange(orginalObj,0,orginalObj.length); 	
				byte[] allDatas = new byte[tmporiginal.length+ fine2.length];
				System.arraycopy(tmporiginal,0,allDatas,0,tmporiginal.length);
				System.arraycopy(fine2,0,allDatas,tmporiginal.length,fine2.length);
				buffer.update(2,Arrays.copyOfRange(allDatas,0,allDatas.length));	
			}
			
			if(buffer.get(3)==null){
				buffer.update(3,Arrays.copyOfRange(coarse1,0,coarse1.length));
			}else{
				byte[] orginalObj = (byte[])buffer.get(3);	
				byte[] tmporiginal= Arrays.copyOfRange(orginalObj,0,orginalObj.length); 	
				byte[] allDatas = new byte[tmporiginal.length+ coarse1.length];
				System.arraycopy(tmporiginal,0,allDatas,0,tmporiginal.length);
				System.arraycopy(coarse1,0,allDatas,tmporiginal.length,coarse1.length);
				buffer.update(3,Arrays.copyOfRange(allDatas,0,allDatas.length));	
			}
			
			
			if(buffer.get(4)==null){
				buffer.update(4,Arrays.copyOfRange(coarse2,0,coarse2.length));
			}else{
				byte[] orginalObj = (byte[])buffer.get(4);	
				byte[] tmporiginal= Arrays.copyOfRange(orginalObj,0,orginalObj.length); 	
				byte[] allDatas = new byte[tmporiginal.length+ coarse2.length];
				System.arraycopy(tmporiginal,0,allDatas,0,tmporiginal.length);
				System.arraycopy(coarse2,0,allDatas,tmporiginal.length,coarse2.length);
				buffer.update(4,Arrays.copyOfRange(allDatas,0,allDatas.length));	
			}
			
			if(buffer.get(5)==null){
				buffer.update(5,Arrays.copyOfRange(coarse0,0,coarse0.length));
			}else{
				byte[] orginalObj = (byte[])buffer.get(5);	
				byte[] tmporiginal= Arrays.copyOfRange(orginalObj,0,orginalObj.length); 	
				byte[] allDatas = new byte[tmporiginal.length+ coarse0.length];
				System.arraycopy(tmporiginal,0,allDatas,0,tmporiginal.length);
				System.arraycopy(coarse0,0,allDatas,tmporiginal.length,coarse0.length);
				buffer.update(5,Arrays.copyOfRange(allDatas,0,allDatas.length));	
			}	

	}
	
	
	/**
	* This method will be used to merge data of two buffers
	// Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
	*/
	@Override
	public void merge(MutableAggregationBuffer buffer, Row buffer2) {
		

		int newAddedrowNumber = buffer2.getInt(0) ;  
		buffer.update(0, buffer.get(0)+","+String.valueOf(newAddedrowNumber));		
		
		for(int i=1;i<=5;i++){
			byte[] inputObj1 = (byte[])buffer2.get(i);	
			byte[] tmp1= Arrays.copyOfRange(inputObj1,0,inputObj1.length);
			byte[] inputObj2 = (byte[])buffer.get(i);	
			byte[] tmp2 = Arrays.copyOfRange(inputObj2,0,inputObj2.length);
			byte[] finalByteArr = new byte[tmp1.length+tmp2.length];
			System.arraycopy(tmp2,0,finalByteArr,0,tmp2.length);
			System.arraycopy(tmp1,0,finalByteArr,tmp2.length,tmp1.length);
			buffer.update(i,Arrays.copyOfRange(finalByteArr,0,finalByteArr.length));				
		}
	}


     private int getTotalRows(String rowsAggreated){
		int count =0;
		for(int j=0;j<rowsAggreated.length();j++){
		if(rowsAggreated.charAt(j)==','){
			count=count+1;
			}
		}
		if(count<=0){
			return 0;
		}
		return count;
	 }
	 
	 
	  private int getBasicRowNumber(String rowsAggreated){
		  
			rowsAggreated = rowsAggreated.replaceFirst(","," ").trim();	
			String[] zz = rowsAggreated.split(",");
			List<Integer> rowNumberArr = new ArrayList<Integer>();			//rowNumberArr stores all row keys
			for(int i=0;i<zz.length;i++){
			rowNumberArr.add(Integer.parseInt(zz[i]));
			}
			int min	= rowNumberArr.stream().mapToInt(v -> v).min().orElse(0);	
			return min;
	  }
	 
	
	//通过行号获取某一行数据 	
    private byte[] getBytesByRowNumber(byte[] allpixelsInWindow, int targetRow, int winSize){
		int bytesPerRow = allpixelsInWindow.length/winSize;
		byte[] rawData = new byte[bytesPerRow];
		byte[] tmp4bytes = new byte[4];
		for(int i=0;i<winSize;i++){
			System.arraycopy(allpixelsInWindow,i*bytesPerRow,rawData,0,rawData.length); 
			System.arraycopy(rawData,rawData.length-4,tmp4bytes,0,tmp4bytes.length);
			int rowNumber =  ByteBuffer.wrap(tmp4bytes).getInt();
			if(rowNumber==targetRow){
				return rawData;
			}
		}
		return null;
	}
	
	//get the value of a specific column of a row 
	private int getCellIntgerValueByCloumn(byte[] allpixelInRow, int targetCloumn){
		byte[] tmp4bytes = new byte[4];
		if(targetCloumn > (allpixelInRow.length-4)/4){
			return 0;
		}
		else{
			System.arraycopy(allpixelInRow,targetCloumn*4,tmp4bytes,0,tmp4bytes.length);
			return ByteBuffer.wrap(tmp4bytes).getInt();	
		}

		
	}
	
	//寻找与指定行列相似的所有粗精分辨率像素值，存放在数组中
	private void getSimlarPixels(Row buffer, int targetRow, int targetColumn, int winSize,int[] Fi ,int[] Ci ){
		//int[] Fi = new int[24*2];			// tm,tn时刻fine中的像素值 默认初始化都是0
		//int[] Ci = new int[24*2];			// tm,tn时刻coarse中的像素值
		Arrays.fill(Fi,0);
		Arrays.fill(Ci,0);
		byte[] tmpobj = (byte[])buffer.get(1);	
		byte[] fine1= Arrays.copyOfRange(tmpobj,0,tmpobj.length);
	    byte[] specifiedRowofFine1 = getBytesByRowNumber(fine1,targetRow,winSize);
		int tmpCellValue =  getCellIntgerValueByCloumn(specifiedRowofFine1,targetColumn );
		
		byte[] tmpobj1 = (byte[])buffer.get(2);	
		byte[] fine2= Arrays.copyOfRange(tmpobj1,0,tmpobj1.length);
		byte[] specifiedRowofFine2 = getBytesByRowNumber(fine2,targetRow,winSize);
		
		byte[] tmpobj2 = (byte[])buffer.get(3);
		byte[] coarse1= Arrays.copyOfRange(tmpobj2,0,tmpobj2.length); 
		byte[] specifiedRowofCoarse1 = getBytesByRowNumber(coarse1,targetRow,winSize);
		
		
		byte[] tmpobj3 = (byte[])buffer.get(4);
		byte[] coarse2= Arrays.copyOfRange(tmpobj3,0,tmpobj3.length); 
		byte[] specifiedRowofCoarse2 = getBytesByRowNumber(coarse2,targetRow,winSize);
		

		if(tmpCellValue==0){
			return ;
		}
		//首先在同行的邻居之间查找
		for(int columni = targetColumn+1; columni<targetColumn+5;columni++){
			
			int inLoopCellValue = getCellIntgerValueByCloumn(specifiedRowofFine1, columni); 			
			if(Math.abs(tmpCellValue-inLoopCellValue)<=thread1V){
				int tmpV1 =getCellIntgerValueByCloumn(specifiedRowofFine2,targetColumn);
				if(tmpV1==0){
					continue;
				}				
				int tmpV2 = getCellIntgerValueByCloumn(specifiedRowofFine2,columni);
				if(Math.abs(tmpV1-tmpV2)<=thread2V){
					int tmplocation = columni-targetColumn;
					Fi[(tmplocation-1)*2]=inLoopCellValue;
					Fi[(tmplocation-1)*2+1]=tmpV2;
					int tmpV3 = getCellIntgerValueByCloumn(specifiedRowofCoarse1,columni);
					int tmpV4 = getCellIntgerValueByCloumn(specifiedRowofCoarse2,columni);
					Ci[(tmplocation-1)*2]=tmpV3;
					Ci[(tmplocation-1)*2+1]=tmpV4;				
				}
			}
		}
		
		for(int inForRow = (targetRow+1); inForRow<=(targetRow+winSize-1); inForRow++){  //在另外的每行中寻找相似像素
			int refRowNumber = inForRow;
			byte[] inloopSpecifiedRowOfFine1 = getBytesByRowNumber(fine1,refRowNumber,winSize);
			for(int columni = targetColumn; columni< targetColumn+5; columni++){
				
				int tmpv1 = getCellIntgerValueByCloumn(inloopSpecifiedRowOfFine1,columni);		
				if(tmpv1==0){
					continue;
				}
				if(Math.abs(tmpCellValue-tmpv1)<=thread1V){
					
					byte[] inloopSpecifiedRowOfFine2 = getBytesByRowNumber(fine2,refRowNumber,winSize);
					int tmpv2 = getCellIntgerValueByCloumn(inloopSpecifiedRowOfFine2,columni);	
					if(tmpv2==0){
						continue;
					}
					int tmpV3 = getCellIntgerValueByCloumn(specifiedRowofFine2, targetColumn);
					if( Math.abs(tmpV3-tmpv2)<=thread2V){
						int tmplocation = 8+ ((refRowNumber-targetRow-1)*10);
						Fi[tmplocation] =  tmpv1;
						Fi[tmplocation+1] =  tmpv2;
						byte[] inCorrespondingCorse1Row =  getBytesByRowNumber(coarse1,refRowNumber,winSize);
						int tmpV5 = getCellIntgerValueByCloumn(inCorrespondingCorse1Row,columni );
						byte[] inCorrespondingCorse2Row =  getBytesByRowNumber(coarse2,refRowNumber,winSize);
						int tmpV6 = getCellIntgerValueByCloumn(inCorrespondingCorse2Row,columni );
						Ci[tmplocation] =  tmpV5;
						Ci[tmplocation+1] =  tmpV6;
					}
				}
			}
		}
	}




 // function that returns correlation coefficient. 
    private  float correlationCoefficient(int X[], int Y[], int n) 
    { 
       
        int sum_X = 0, sum_Y = 0, sum_XY = 0; 
        int squareSum_X = 0, squareSum_Y = 0; 
       
        for (int i = 0; i < n; i++) 
        { 
            // sum of elements of array X. 
            sum_X = sum_X + X[i]; 
       
            // sum of elements of array Y. 
            sum_Y = sum_Y + Y[i]; 
       
            // sum of X[i] * Y[i]. 
            sum_XY = sum_XY + X[i] * Y[i]; 
       
            // sum of square of array elements. 
            squareSum_X = squareSum_X + X[i] * X[i]; 
            squareSum_Y = squareSum_Y + Y[i] * Y[i]; 
        } 
       
        // use formula for calculating correlation  
        // coefficient. 
        float corr = (float)(n * sum_XY - sum_X * sum_Y)/ 
                     (float)(Math.sqrt((n * squareSum_X - 
                     sum_X * sum_X) * (n * squareSum_Y -  
                     sum_Y * sum_Y))); 
       
        return corr; 
    } 


	
	//得到最终结果时会调用的执行函数，直接计算这几个区域的值
	@Override
	public Object evaluate(Row buffer) {
		
		String rowsAggreated=buffer.get(0).toString();
		int rowCount=getTotalRows(rowsAggreated); 						//其实这个rowCount就是窗口的大小winSize
		int winSize = rowCount;
		baselineRowNumber = getBasicRowNumber(rowsAggreated);			//最小行就是索引行，要依据此进行计算
		byte[] tmpobj = (byte[])buffer.get(5);							//得到所有coarse0窗口数据
		byte[] coarse0= Arrays.copyOfRange(tmpobj,0,tmpobj.length);	    
		byte[] coase0Baserow = getBytesByRowNumber(coarse0,baselineRowNumber,winSize);


		byte[] tmpobj2 = (byte[])buffer.get(1);							//得到所有fine1窗口数据
		byte[] fine1= Arrays.copyOfRange(tmpobj2,0,tmpobj2.length);
		
		byte[] tmpobj3 = (byte[])buffer.get(2);							//得到所有fine2窗口数据
		byte[] fine2= Arrays.copyOfRange(tmpobj3,0,tmpobj3.length);	
		
				
		byte[] tmpobj1 = (byte[])buffer.get(3);							//得到所有coarse1窗口数据
		byte[] coarse1= Arrays.copyOfRange(tmpobj1,0,tmpobj1.length);	   
		
		byte[] tmpobj4 = (byte[])buffer.get(4);							//得到所有fine2窗口数据
		byte[] coarse2= Arrays.copyOfRange(tmpobj4,0,tmpobj4.length);	
		
		
		

		float[] Di = new float[24];	
		float[] Wi = new float[24];			//Weigth
		int[] Fi = new int[24*2];			// tm,tn时刻fine中的像素值
		int[] Ci = new int[24*2];			// tm,tn时刻coarse中的像素值

		int[] distanceReferenceTable = new int[]{1,4,9,16,1,2,5,10,17,4,5,8,13,20,
		9,10,13,18,25,16,17,20,25,32,25,26,29,34,41};
		
		
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		
		
		for(int i=0;i<(coase0Baserow.length-4)/4;i++){
			
			int ppRow = baselineRowNumber;
			int ppColum = i;
			getSimlarPixels(buffer, ppRow, ppColum, winSize,Fi,Ci);			//这个地方可能有问题	
			
			double[] tmpdoubleX = new double[48];
			double[] tmpdoubleY = new double[48];
			for(int innneriii =0;innneriii<Fi.length;innneriii++){
				tmpdoubleX[innneriii]=Fi[innneriii];
				tmpdoubleY[innneriii]=Ci[innneriii];
			}
			
			RegressionModel model = new LinearRegressionModel(tmpdoubleX, tmpdoubleY);     
			model.compute();  
			double[] coefficients = model.getCoefficients();
			float V =(float)coefficients[1];			// LinearRegressionModel slope	
			for(int j=0;j<Wi.length;j++){				//所有相邻像素都对应一个权重
				int X[] = new int[2];
				int Y[] = new int[2];
				X[0] = Fi[j*2];
				X[1] = Fi[j*2+1];
				Y[0] = Ci[j*2];
				Y[1] = Ci[j*2+1];
				float Ri = correlationCoefficient(X,Y,X.length);
				float di = 1+ (float)Math.sqrt(distanceReferenceTable[j])/(windowSize/2);
				Di[j] = (1-Ri)*di;
			}
			
			
			//从Di得到Wi, 
			float tmpsumdi =0.0f;
			for(int innerDi =0;innerDi<Di.length;innerDi++){
				tmpsumdi = tmpsumdi+ 1/Di[innerDi];
			}
			for(int innerWi =0;innerWi<Wi.length;innerWi++){
				Wi[innerWi]= (1/Di[innerWi])/tmpsumdi;
			}
			
			
			//这里开始为每个coarse0中的每个像素预估结果
			//计算全部窗口内的值
			int tk=0; //k--> m  k-->n 
			int tkm =0;  //coase1
			int tkn=0;
			int tp =0;
			float Tm =0.0f;
			float Tn =0.0f;

			for(int innerI=ppRow; innerI<ppRow+winSize;innerI++){
				byte[] innerRowBytes =  getBytesByRowNumber(coarse0,innerI,winSize);
				for(int innerJ=ppColum; innerJ<ppColum+5;innerJ++){
					tp = tp+getCellIntgerValueByCloumn(innerRowBytes,innerJ);
				}
			}		
			for(int innerI=ppRow; innerI<ppRow+winSize;innerI++){
				byte[] innerRowBytes =  getBytesByRowNumber(coarse1,innerI,winSize);
				for(int innerJ=ppColum; innerJ<ppColum+5;innerJ++){
					tkm = tkm+getCellIntgerValueByCloumn(innerRowBytes,innerJ);
				}
			}
			for(int innerI=ppRow; innerI<ppRow+winSize;innerI++){
				byte[] innerRowBytes =  getBytesByRowNumber(coarse2,innerI,winSize);
				for(int innerJ=ppColum; innerJ<ppColum+5;innerJ++){
					tkn = tkm+getCellIntgerValueByCloumn(innerRowBytes,innerJ);
				}
			}
			if(tkm-tp==0 || tkn-tp==0) {
				Tm=0;
				Tn=0;		
			}
			else{
				Tm = (1/Math.abs(tkm-tp))/(1/Math.abs(tkm-tp)+ 1/Math.abs(tkn-tp));
				Tn =  (1/Math.abs(tkn-tp))/(1/Math.abs(tkm-tp)+ 1/Math.abs(tkn-tp));
			}

			
			
			//首先从Fine1, Fine2的（ij）处取值，然后使用其周围的24个相似像素进行计算
			byte[] tmpRowBytes1 = getBytesByRowNumber(fine1,ppRow, winSize);
			int tmpfine01 = getCellIntgerValueByCloumn(tmpRowBytes1, ppColum);
			
			byte[] tmpRowBytes2 = getBytesByRowNumber(fine2,ppRow, winSize);
			int tmpfine02 = getCellIntgerValueByCloumn(tmpRowBytes2, ppColum);
			
			float WVcoase0SubstractCoarse1 = 0.0f;
			for(int innerI=ppRow; innerI<ppRow+winSize;innerI++){
				if(innerI==ppRow){
					byte[] innerRowBytesCoarse0 =  getBytesByRowNumber(coarse0,innerI,winSize);
					byte[] innerRowBytesCoarse1  =  getBytesByRowNumber(coarse1,innerI,winSize);
					for(int innerJ =ppColum+1;innerJ<ppColum+5; innerJ++){
						int tmpV1 = getCellIntgerValueByCloumn(innerRowBytesCoarse0,innerJ);
						int tmpV2 = getCellIntgerValueByCloumn(innerRowBytesCoarse1,innerJ);
						int locationWi= innerJ-ppColum-1;
						WVcoase0SubstractCoarse1 = WVcoase0SubstractCoarse1+ Wi[locationWi]*V*(tmpV1-tmpV2);
					}	
				}else{	
					
					int cutRow = innerI;
					byte[] innerRowBytesCoarse0 =  getBytesByRowNumber(coarse0,cutRow,winSize);
					byte[] innerRowBytesCoarse1  =  getBytesByRowNumber(coarse1,cutRow,winSize);
					for(int innerJ =ppColum;innerJ<ppColum+5; innerJ++){
					int tmpV1 = getCellIntgerValueByCloumn(innerRowBytesCoarse0,innerJ);
					int tmpV2 = getCellIntgerValueByCloumn(innerRowBytesCoarse1,innerJ);
					int locationWi= 4+(cutRow-ppRow-1)*5+(innerJ-ppColum);   //第一行的四个加上其它每行都有5个像素
					WVcoase0SubstractCoarse1 = WVcoase0SubstractCoarse1+Wi[locationWi]*V*(tmpV1-tmpV2);
					}
				}	
			}
			
			
			float WVcoase0SubstractCoarse2 = 0.0f;
			for(int innerI=ppRow; innerI<ppRow+winSize;innerI++){
				if(innerI==ppRow){
					byte[] innerRowBytesCoarse0 =  getBytesByRowNumber(coarse0,innerI,winSize);
					byte[] innerRowBytesCoarse2  =  getBytesByRowNumber(coarse2,innerI,winSize);
					for(int innerJ =ppColum+1;innerJ<ppColum+5; innerJ++){
						int tmpV1 = getCellIntgerValueByCloumn(innerRowBytesCoarse0,innerJ);
						int tmpV2 = getCellIntgerValueByCloumn(innerRowBytesCoarse2,innerJ);
						int locationWi= innerJ-ppColum-1;
						WVcoase0SubstractCoarse2 = WVcoase0SubstractCoarse2+ Wi[locationWi]*V*(tmpV1-tmpV2);
					}	
				}else{	
					
					int cutRow = innerI;
					byte[] innerRowBytesCoarse0 =  getBytesByRowNumber(coarse0,cutRow,winSize);
					byte[] innerRowBytesCoarse2  =  getBytesByRowNumber(coarse2,cutRow,winSize);
					for(int innerJ =ppColum;innerJ<ppColum+5; innerJ++){
					int tmpV1 = getCellIntgerValueByCloumn(innerRowBytesCoarse0,innerJ);
					int tmpV2 = getCellIntgerValueByCloumn(innerRowBytesCoarse2,innerJ);
					int locationWi= 4+(cutRow-ppRow-1)*5+(innerJ-ppColum);   //第一行的四个加上其它每行都有5个像素
					WVcoase0SubstractCoarse2 = WVcoase0SubstractCoarse2+Wi[locationWi]*V*(tmpV1-tmpV2);
					}
				}	
			}

			
			//double predicatedVal= fine01+
			
			float fine01 = tmpfine01 + WVcoase0SubstractCoarse1;
			float fine02 = tmpfine02 + WVcoase0SubstractCoarse2;
			
			float fine = fine01*Tm + fine02*Tn;
			byte[] fineBytes= ByteBuffer.allocate(4).putFloat(fine).array();
			output.write(fineBytes,0,fineBytes.length);
		}
		
		
		return output.toByteArray();
	
	}
	
	
	
	
	

		/**
	* This method determines the return type of this UDAF
	*/
	@Override
	public DataType dataType() {
		return  returnDataType;
	}

	/**
	* Returns true iff this function is deterministic, i.e. given the same input, always return the same output.
	*/
	@Override
	public boolean deterministic() {
		return true;
	}

	@Override
	public StructType bufferSchema() {
	// TODO Auto-generated method stub
	return bufferSchema;
	}

	/**
	* This method will determine the input schema of this UDAF
	*/
	@Override
	public StructType inputSchema() {
	return inputSchema;
	}	

}
