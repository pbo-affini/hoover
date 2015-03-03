#! venv/bin/python

import csv
import os
import copy
import multiprocessing
import json
import inspect

#The different kinf of types used. Some other will come later
TYPE_DICT = {1 : "integer", 2 : "float", 3 : "string"}


def convert_to_type(x, type):
    """
    Convert a variable into the given type
    """
    if type == "integer":
        x = int(x)
    if type == "float":
        x = float(x)
    return x

def create_transformation_function(expression):
    """
    Create a function evaluating the given expression and return it
    """

    def transformation(x):
        try:
            result = eval(expression)
        except NameError:
            raise NameError
        return result

    return transformation

class Hoover(object):
    """
    The main class, use to apply all transformations needeed on our file
    """

    def __init__(self, filetoclean, delimiter, filetowrite, nb_process=1):
        """
        A hoover need three informations : the fle to clean, the delimiter in the file
        and the file where to write
        """
        self.filetoclean = filetoclean
        self.delimiter = delimiter
        self.filetowrite = filetowrite
        self.nb_process = nb_process

    def add_header(self, file_has_header, header_file=False, basic_header=False):
        """
        Add the headers to a hoover. It could come from the file, from another file, or from the user.
        It adds in the same time the first line of file.
        """
        print "Addition of headers"
        
        #if we already have a file with header, save with writeinfos fo example
        if header_file:
            with open(self.filetoclean + "_header") as inputfile:
                self.header = json.load(inputfile)
            with open(self.filetoclean + "_first_line") as inputfile:
                self.first_line = json.load(inputfile)
        
        else:
            with open(self.filetoclean) as input_file:
                first_line = input_file.next()
                #if the file already have a line of headers
                if file_has_header:
                    header = first_line.replace('\n','').split(self.delimiter)
                    with open(self.filetoclean + "_copy", "w") as output_file:
                        first_line = input_file.next()
                        output_file.write(first_line)
                        for line in input_file:
                            output_file.write(line)
                    os.rename(self.filetoclean + "_copy",self.filetoclean)
                
                #else we have to manualy add the headers
                else:
                    header = []
                    if not basic_header:
                        print "For each field, print the column name"
                        for i, field in enumerate(first_line.split(self.delimiter)):
                            print("Field " + str(i) + " : "),
                            header.append(raw_input())
                    #if you just want to have field 0, field 1... as header
                    else:
                        header = ["field " + str(i) for i in range(0, len(first_line.split(self.delimiter)))]
                self.header = header
                self.first_line = first_line.replace('\n','').split(self.delimiter)

    def add_type(self, list_type_file=False):
        """
        Add the different types of data of the file to the hoover.
        It could come from an external file or from the user
        """
        print "Addition of types"

        #if we already have a type file
        if list_type_file:
            with open(self.filetoclean + "_type_list") as inputfile:
                self.type_list = json.load(inputfile)
        
        #else we have to add it manually
        else:
            type_list = []
            for key, type in TYPE_DICT.items():
                print "%s : %s" %(key, type)
            print "For each field, type the type of the field"
            for field in self.header:
                while True:
                    print(str(field) + " : "),
                    try:
                        type_list.append(TYPE_DICT[input()])
                        break
                    except KeyError:
                        print "Invalid number. Please retry"
                    except NameError:
                        print "Invalid name. Please retry"
            self.type_list = type_list

    def add_transformation(self, functions_dict=None, field_to_transform=None,file_transformation=False):
        """
        Add the transformations to apply to each field of the file.
        The transformations are caught from the user (with a transformation to apply to x)
        or provided with a dictionnary of functions. It is also possible to transform only
        a subset of field (provided with the field_to_transform list) and let the other unchanged.
        Finally, it is possible to provide the transformations from a file
        """
        print "Addition of transformations"
        #the transformation list contains object functions to apply to file
        transformation_list = []
        #the transformation code contains the code of the previous function, in order to save them in a file
        transformation_code = []

        #if we already have a transformation file
        if file_transformation:
            with open(self.filetoclean + "_transformation_code") as inputfile:
                for i, code in enumerate(json.load(inputfile)):
                    transformation_code.append(code)
                    #in this case, we have a UDF, and must just evaluate the code
                    if "def" in code:
                        exec(code)
                        func_name = code.split(' ')[1].split('(')[0]
                        transformation_list.append(eval(func_name))
                    #in this case, the function is juste the eval of the input, and we must reconstruct it
                    else:
                        transformation_list.append(create_transformation_function(code))
        #else we have to ask the transformations to the user
        else:
            print "For each field x, print the transformation to apply to x"
            for i, field in enumerate(self.header):
                
                #here we have a field that we don't want to transform
                if field_to_transform and i not in field_to_transform:
                    transformation_list.append(create_transformation_function("x"))
                    continue
                while True:
                    print(str(field) + " : "),
                    function = raw_input()
                    #if we have an UDF, we just append it to the transformation_list
                    if functions_dict and function in functions_dict:
                        transformation_list.append(functions_dict[function])
                        transformation_code.append("".join(inspect.getsourcelines(functions_dict[function])[0]))
                    else:
                        transformation_list.append(create_transformation_function(function))
                        transformation_code.append(function)
                    try:
                        """we try to apply the function created to the concerned field of the first line
                        it allows to verify that the syntax is correct, and that it perform the desired transformation,
                        asking to the user if the result is good"""
                        result = transformation_list[i](convert_to_type(self.first_line[i], self.type_list[i]))
                        print "The result will for this field for the value %s will be %s, is this correct ? [y/n]" %(self.first_line[i],result)
                        agree = raw_input()
                        if agree == "y":
                            break
                        #if the user aggrees not the result, or if there is a syntax error, we remove the function
                        #from the transformations list, and we ask again the function to the user
                        transformation_list.pop()
                        transformation_code.pop()
                    except SyntaxError:
                        print "The function you write has an invalid syntax. Please retry"
                        transformation_list.pop()
                        transformation_code.pop()
                    except NameError:
                        print "The function you write has an invalid name. Please retry"
                        transformation_code.pop()
                        transformation_list.pop()
        self.transformation_code = transformation_code
        self.transformation_list = transformation_list

    def apply_line_transformation(self, line):
        """
        Apply the transformations given to a line
        """
        line_result = []
        for i, field in enumerate(line):
            line_result.append(self.transformation_list[i](convert_to_type(field, self.type_list[i])))
        return line_result

    def apply_file_transformation(self):
        """
        Apply the transformations to the entirely input file
        """
        print "Begin the file transformation"
        error_exist = False
        with open(self.filetoclean) as input_file:
            with open(self.filetowrite,"w") as output_file:
                #this file allows to store errors and evitate to stop the script at the first error
                with open(self.filetowrite + "_error","w") as output_file_error:
                    reader = csv.reader(input_file, delimiter = self.delimiter)
                    writer = csv.writer(output_file, delimiter = self.delimiter, lineterminator="\n")
                    writer_error = csv.writer(output_file_error, delimiter = self.delimiter, lineterminator="\n")
                    for line in reader:
                        try:
                            writer.writerow(self.apply_line_transformation(line))
                        except:
                            error_exist = True
                            writer_error.writerow(line)
        #if there were no errors, we remove the error file
        if not error_exist:
            os.remove(self.filetowrite + "_error")

    def separate_into_chunks(self):
        """
        Separate the file to clean in several smaller file, in order to parallelize the process
        """
        print "Begin the transformation into chunks"
        nb_lines = sum(1 for line in open(self.filetoclean))
        nb_lines_by_chunk = nb_lines/self.nb_process
        with open(self.filetoclean) as input_file:
            for i in range(0, self.nb_process):
                with open(self.filetoclean + "_" + str(i), "w") as outputfile:
                    j = 0
                    for line in input_file:
                        outputfile.write(line)
                        j = j+1
                        if j == nb_lines_by_chunk:
                            break
                    if i == self.nb_process - 1:
                        for line in input_file:
                            outputfile.write(line)

    def apply_file_transformation_multiprocess(self):
        """
        Apply the transformation to the file in parallel
        """
        print "Begin the transformation in multiprocess"
        list_process = []
        for i in range(0, self.nb_process):
            #for each process, we create a new hoover object, the copy of the main object, and we just change the file attributes
            c = copy.copy(self)
            c.filetoclean = c.filetoclean + "_" + str(i)
            c.filetowrite = c.filetowrite + "_" + str(i)
            p = multiprocessing.Process(target = c.apply_file_transformation)
            list_process.append(p)
            p.start()
        for p in list_process:
            p.join()

    def reassemblate_chunks(self):
        """
        Reassemblate the different chunks of cleaned file a one file
        """
        print "Begin the assemmbly of chunks"
        with open(self.filetowrite, "w") as output_file:
            for i in range(0, self.nb_process):
                with open(self.filetowrite + "_" + str(i)) as input_file:
                    for line in input_file:
                        output_file.write(line)
                os.remove(self.filetoclean + "_" + str(i))
                os.remove(self.filetowrite + "_" + str(i))
        #here we reaasemblate the error file, then we delete them
        error_exist = False
        with open(self.filetowrite + "_error", "w") as error_file:
            for i in range(0, self.nb_process):
                if os.path.exists(self.filetowrite + "_" + str(i) + "_error"):
                    error_exist = True
                    with open(self.filetowrite + "_" + str(i) + "_error") as chunk_error_file:
                        for line in chunk_error_file:
                            error_file.write(line)
                    os.remove(self.filetowrite + "_" + str(i) + "_error")
        if not error_exist:
            os.remove(self.filetowrite + "_error")
   
    def write_infos(self, header, first_line, type_list, transformation_list):
        """
        Write the different informations provided by user in file, in order to reuse it
        without catch them a other time
        """
        print "Begin the write of infos"
        if header:
            with open(self.filetoclean + "_header","w") as header_file:
                json.dump(self.header,header_file)
        if first_line:
            with open(self.filetoclean + "_first_line","w") as first_line_file:
                json.dump(self.first_line,first_line_file)
        if type_list:
            with open(self.filetoclean + "_type_list","w") as type_file:
                json.dump(self.type_list, type_file)
        if transformation_list:
            with open(self.filetoclean + "_transformation_code","w") as transformation_file:
                json.dump(self.transformation_code, transformation_file)

    def write_with_header(self):
        """
        Rewrite the cleaned file with headers
        """
        print "Begin the wirte with headers"
        with open(self.filetowrite) as input_file:
            with open(self.filetowrite + "_copy", "w") as outputfile:
                outputfile.write(self.delimiter.join(self.header) + "\n")
                for line in input_file:
                    outputfile.write(line)
        os.rename(self.filetowrite + "_copy",self.filetowrite)
