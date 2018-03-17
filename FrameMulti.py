#object definition for our frame class
#output: feature class with polygon for each frame with its determined ratio value

import arcpy, collections, os, time, threading
from multiprocessing import Pool #Multithreading using map and pool
#https://stackoverflow.com/questions/2846653/how-to-use-threading-in-python

class classifiedRaster: #class definition for the frames made from the whole raster
    
	def __init__(self, in_ras, in_sizeX, in_sizeY, in_ratio,in_classification): #inputs: main raster, frame size (5m x 10m), ratio (80%), and desired classification (weeds).
		self.__inras = in_ras
		self.__frameX = float(in_sizeX)
		self.__frameY = float(in_sizeY)
		self.__frame_ratio = float(in_ratio)
		self.__in_class = in_classification
		self.__max_y = float(arcpy.GetRasterProperties_management(in_ras, "TOP").getOutput(0))
		self.__min_y = float(arcpy.GetRasterProperties_management(in_ras, "BOTTOM").getOutput(0))
		self.__max_x = float(arcpy.GetRasterProperties_management(in_ras, "RIGHT").getOutput(0))
		self.__min_x = float(arcpy.GetRasterProperties_management(in_ras, "LEFT").getOutput(0))

	def processRaster(self,output, User_Field_Count, Class_List, User_Field, Fields_List, Window_Overlap):
		arcpy.AddMessage("Processing raster.")
		fc = output  #this is the path to shapefile made by the user in the tool box
		arcpy.env.overwriteOutput = True

		#arcpy.AddMessage(str(arcpy.env.workspace) +"  "+ str(os.path.split(output)[1]))
		#arcpy.AddMessage(str(fc))
		#arcpy.AddMessage(arcpy.Exists(output))
		#arcpy.AddMessage(str(output))
			
		arcpy.management.CreateFeatureclass(arcpy.env.workspace,os.path.split(output)[1],"POLYGON")
		frame = "TempClip"#defining the location where the temporary frame will be saved for cutting out what we need
			
		R="Ratio" #name for field in attribute table
		F="FLOAT" #data type for attribute table
		arcpy.management.AddField(fc,R,F) #adding properties to attribute table
		arcpy.management.AddField(fc,"X",F) #adding properties to attribute table for coordinates for each poygon made
		arcpy.management.AddField(fc,"Y",F) #adding properties to attribute table for coordinates for each poygon made
		
		projection = arcpy.Describe(self.__inras).spatialReference #Assigning the feature class the same projection as inras
		arcpy.DefineProjection_management(fc,projection)
		
		cursor = arcpy.da.InsertCursor(fc, ["SHAPE@","Ratio","X","Y"]) #cursor for creating the valid frame feature class
		#arcpy.AddMessage("Passed the cursor")
		y = float(self.__min_y) #set to bottom of in raster
			
		frameCount = 0 #some nice counters for output while processing
		validFrameCount = 0
		
		#fixing total frame calculation
		yDiff = ((self.__max_y-self.__min_y)/self.__frameY*2)
		xDiff = ((self.__max_x-self.__min_x)/self.__frameX*2)
		if yDiff %1 != yDiff:
			yDiff += 1
		if xDiff %1 != xDiff:
			xDiff += 1
		
		totalFrames = int(xDiff * yDiff)
		
		start_time = time.clock()
		run_time = 0
		time_counter = 0
		error_count = 0
		
		# make the Pool of workers. Number can be adjusted. DO NOT MAKE MORE THAN 10 (Will break the file overwriting, potentially corrupt processing. Can fix if needed)
		pool = ThreadPool(3) 
		
		#results = pool.map(my_function, my_array) should be an array of valid frames, probably don't need to use results
		#my_function = processFrame, my_array will be list of frames. Create array of arrays?
		#list.append(elem) add next frame
		rectArray = []
	
		try:
			while(y < self.__max_y):#flow control based on raster size and requested frame size needed. Issue on edges, ask about.
				x = float(self.__min_x) #set to left bound of in raster
				#arcpy.AddMessage("Passed 1 while")
				#arcpy.AddMessage("x = " +str(x))
				#arcpy.AddMessage("max X = " +str(self.__max_x))
				try:
					while (x < self.__max_x): #"side to side" processing
						#arcpy.AddMessage("Passed 2 while")				
						rectangle = str(x) + " " + str(y) + " " + str(x + self.__frameX) + " " + str(y + self.__frameY) #bounds of our frame for the clip tool						
						rectArray.append(rectangle) #add frame to array of frames
						frameCount += 1 #updating processing counter, this might be broken with threading
						
						arcpy.AddMessage("Frame " + frameCount + " added to queue")
						
						x = int(x) + int(float(self.__frameX)*Window_Overlap)#move user-determined distance to the side

				except:
					arcpy.AddMessage("Frame failed to process.")
					error_count += 1
				y = float(y) + int(float(self.__frameY)*Window_Overlap)#move user-determined distance up
		except:
			error_count += 1
		(self.__inras,rectangle, frame)
		
		
		arcpy.AddMessage("Processing frames") #Potential overwrite issues since same frame location.
		pool.starmap(self.processFrame,zip(itertools.repeat(cursor),rectArray, itertools.repeat(frame), itertools.repeat(User_Field_Count), itertools.repeat(Class_List), itertools.repeat(User_Field), itertools.repeat(Fields_List)))
		arcpy.AddMessage("Frames processed")
		del cursor #prevent data corruption by deleting cursor when finished		 
				
	def processFrame(self, cursor, rectangle, frame, User_Field_Count, Class_List, User_Field, Fields_List): #needs to create the frame and add it using cursor if valid
		try:
			processName = multiprocessing.current_process() #returns worker name and other status information
			processFrame = frame + processName[20] #should return worker number and fix overwrite issue
			arcpy.Clip_management(self.__inras,rectangle, processFrame)#create frame -> clip out a section of the main raster 
			arcpy.AddMessage("Process " + processName[20] + " has clipped frame " + rectangle)
			
			
			x = float(arcpy.GetRasterProperties_management(in_ras, "LEFT").getOutput(0))
			y = float(arcpy.GetRasterProperties_management(in_ras, "BOTTOM").getOutput(0))
			try:				
				validFrame, validRatio = density(frame, self.__frame_ratio, self.__in_class, User_Field_Count, Class_List, User_Field, Fields_List) #run ratio function. Expect boolean T if frame meets ratio conditions, and actual ratio
				if validFrame: #Case it passes
					array = arcpy.Array([arcpy.Point(x, y), arcpy.Point(x, y + self.__frameY),arcpy.Point(x + self.__frameX, y + self.__frameY),arcpy.Point(x + self.__frameX, y)]) #creating the frame polygon
					polygon = arcpy.Polygon(array)
					lat = y+self.__frameY/2
					long = x+self.__frameX/2
					cursor.insertRow([polygon,validRatio, lat, long]) #add frame to feature class with calculated attributes
					arcpy.AddMessage("Process " + processName[20] + " has found valid frame at " + rectangle)
			except:
				arcpy.AddMessage("Failed to process frame. Exception 1")
		except:
			arcpy.AddMessage("Failed to process frame. Exception 2")

				
def density(inras, ratio, inclass, User_Field_Count, Class_List, User_Field_Value,Fields_List): #determines ratio of classification
	fc = inras #Determines file path from user input
	#arcpy.AddMessage("Processing frame.")
	#arcpy.AddMessage("fc = " + str(fc))
	
	countField= User_Field_Count
	arcpy.BuildRasterAttributeTable_management(fc, "Overwrite") #updates attribute table to reflect frame, rather than whole
	cursor = arcpy.SearchCursor(fc,Fields_List)
	
	frequency = 0 #counters
	total = 0

	for row in cursor: #Calculates information on each classification
		#arcpy.AddMessage("inclass = " + str(inclass))
		#arcpy.AddMessage("row.getValue(User_Field_Value) = " + str(row.getValue(User_Field_Value)))
		#arcpy.AddMessage("row[ValueColumn] = " + str(row[ValueColumn]))
		try:
			#if row[ValueColumn] == inclass: #calc frequency of the classification requested
			#Add if statement to filter out frames with null values ("off raster" when angled)
			#Potentially use Is Null to generate 
			if int(row.getValue(User_Field_Value)) == int(inclass): #calc frequency of the classification requested
				frequency = row.getValue(User_Field_Count)
				#arcpy.AddMessage("Frequency = " + str(row.getValue(User_Field_Count)))
			total += row.getValue(User_Field_Count) #calculates sum
			#bug fix to do: calculate total based on area of frame, rather than total classified pixels
			#will fix the issue where we have a frame out in the middle of nowhere
			#arcpy.AddMessage("Total = " + str(total))
		except:
			#arcpy.AddMessage("Not in frame.")
			return False, 0
		
	if total == 0: #preventing dividing by 0, case where there is nothing in the frame
		#arcpy.AddMessage("Frame empty.") #Potentially adjust angle of rectangle to match that of the raster, would save processing
		return False, 0
	
	final_ratio = float(frequency)/float(total) #Calculates ratio for user input classification
	#arcpy.AddMessage("Frame has density = " + str(final_ratio))
	if final_ratio >= ratio: 
		return True, final_ratio #Returns true and final ratio if user input is met
	else:
		return False, final_ratio #Returns false and final ratio if user input is not met

