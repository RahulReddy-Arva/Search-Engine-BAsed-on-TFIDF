The execution of all the programs is done in Eclipse IDE. So, the instructions will be based on that. The same code can be executed even in hadoop with the same arguments. I have added all the required jar files from hadoop environment. We have to add the command line arguments in the Run -> Run Configurations window. Follow the below argument patterns for the respective files. 


Instructions:

DocWordCount.java:
 (There are 2 arguments for this class : args[0],args[1])

1. args[0]: The path of input files directory is passed here.
	Ex: The path in my machine is /home/cloudera/Canterbury/*
2. args[1]: The path where the output file is to be stored is given here.
	Ex: /home/cloudera/Output/DocWordCount
3. Check and verify for the output file generated in the path specified in args[1]



TermFrequency.java :
(There are 2 arguments for this class : args[0],args[1])

1. args[0]: The path of input files directory is passed here.
	Ex: The path in my machine is /home/cloudera/Canterbury/*
2. args[1]: The path where the output file is to be stored is given here.
	Ex: /home/cloudera/Output/TermFrequency
3. Check and verify for the output file generated in the path specified in args[1]



TFIDF.java:
 (There are 3 arguments for this class : args[0],args[1],args[2])

1. args[0]: The path of input files directory is passed here.
	Ex: The path in my machine is /home/cloudera/Canterbury/*
2. args[1]: Pass the output directory for term frequency output.
	Ex: /home/cloudera/output
3. args[2]: Pass the path for the final output file.
	Ex: /home/cloudera/output1
4. Check and the verify the generated in the path specified in args[2].



Search.java:
(There are 3 arguments for this class : args[0],args[1],arg[2])

1. args[0]: The path of input files directory is passed here which is the output file of TFIDF.
	Ex: The path in my machine is /home/cloudera/output1
2. args[1]: Pass the output directory for output.
	Ex: /home/cloudera/output2
3. args[2]: Enter the search key. 
	In our case it is 	i) "computer science"
				ii) "data analysis"
				
4. Check and the verify the generated in the path specified in args[1].
5. Repeat the process separately for each search key.








