#! venv/bin/python

import csv
import os
import copy
import multiprocessing
import json
import inspect

TYPE_DICT = {1 : "integer", 2 : "float", 3 : "string"}

def convert_to_type(x, type):
    if type == "integer":
        x = int(x)
    if type == "float":
        x = float(x)
    return x

def create_transformation_function(expression):

    def transformation(x):
        try:
            result = eval(expression)
        except NameError:
            raise NameError
        return result

    return transformation

class Hoover(object):

    def __init__(self, filetoclean, delimiter, filetowrite, nb_process=1):
        self.filetoclean = filetoclean
        self.delimiter = delimiter
        self.filetowrite = filetowrite
        self.nb_process = nb_process

    def add_header(self, has_header, header_file=False):
        print "Addition of headers"
        if header_file:
            with open(self.filetoclean + "_header") as inputfile:
                self.header = json.load(inputfile)
            with open(self.filetoclean + "_first_line") as inputfile:
                self.first_line = json.load(inputfile)
        else:
            with open(self.filetoclean) as input_file:
                first_line = input_file.next()
                if has_header:
                    header = first_line.replace('\n','').split(self.delimiter)
                    with open(self.filetoclean + "_copy", "w") as output_file:
                        first_line = input_file.next()
                        output_file.write(first_line)
                        for line in input_file:
                            output_file.write(line)
                    os.rename(self.filetoclean + "_copy",self.filetoclean)
                else:
                    header = []
                    print "Do you want to add column name ? [y/n]"
                    agree = raw_input()
                    if agree == "y":
                        print "For each field, print the column name"
                        for i, field in enumerate(first_line.split(self.delimiter)):
                            print("Field " + str(i) + " : "),
                            header.append(raw_input())
                    else:
                        header = ["field " + str(i) for i in range(0, len(first_line.split(self.delimiter)))]
                self.header = header
                self.first_line = first_line.replace('\n','').split(self.delimiter)

    def add_type(self, list_type_file=False):
        print "Addition of types"
        if list_type_file:
            with open(self.filetoclean + "_type_list") as inputfile:
                self.type_list = json.load(inputfile)
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
        print "Addition of transformations"
        transformation_list = []
        transformation_code = []
        if file_transformation:
            with open(self.filetoclean + "_transformation_code") as inputfile:
                for i, code in enumerate(json.load(inputfile)):
                    transformation_code.append(code)
                    print self.type_list
                    print str(i)
                    if "def" in code:
                        exec(code)
                        func_name = code.split(' ')[1].split('(')[0]
                        transformation_list.append(eval(func_name))
                    else:
                        transformation_list.append(create_transformation_function(code))
        else:
            print "For each field x, print the transformation to apply to x"
            for i, field in enumerate(self.header):
                if field_to_transform and i not in field_to_transform:
                    transformation_list.append(create_transformation_function("x"))
                    continue
                while True:
                    print(str(field) + " : "),
                    function = raw_input()
                    if functions_dict and function in functions_dict:
                        transformation_list.append(functions_dict[function])
                        transformation_code.append("".join(inspect.getsourcelines(functions_dict[function])[0]))
                    else:
                        transformation_list.append(create_transformation_function(function))
                        transformation_code.append(function)
                    try:
                        result = transformation_list[i](convert_to_type(self.first_line[i], self.type_list[i]))
                        print "The result will for this field for the value %s will be %s, is this correct ? [y/n]" %(self.first_line[i],result)
                        agree = raw_input()
                        if agree == "y":
                            break
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
        line_result = []
        for i, field in enumerate(line):
            line_result.append(self.transformation_list[i](convert_to_type(field, self.type_list[i])))
        return line_result

    def apply_file_transformation(self):
        print "Begin the file transformation"
        error_exist = False
        with open(self.filetoclean) as input_file:
            with open(self.filetowrite,"w") as output_file:
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
        if not error_exist:
            os.remove(self.filetowrite + "_error")

    def separate_into_chunks(self):
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
        print "Begin the transformation in multiprocess"
        list_process = []
        for i in range(0, self.nb_process):
            c = copy.copy(self)
            c.filetoclean = c.filetoclean + "_" + str(i)
            c.filetowrite = c.filetowrite + "_" + str(i)
            p = multiprocessing.Process(target = c.apply_file_transformation)
            list_process.append(p)
            p.start()
        for p in list_process:
            p.join()

    def reassemblate_chunks(self):
        print "Begin the assemmbly of chunks"
        with open(self.filetowrite, "w") as output_file:
            for i in range(0, self.nb_process):
                with open(self.filetowrite + "_" + str(i)) as input_file:
                    for line in input_file:
                        output_file.write(line)
                os.remove(self.filetoclean + "_" + str(i))
                os.remove(self.filetowrite + "_" + str(i))
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
            #transformation_code = ["".join(inspect.getsourcelines(func)[0]).replace('    def','def').replace('\n        ','\n    ') for func in self.transformation_list]
            with open(self.filetoclean + "_transformation_code","w") as transformation_file:
                json.dump(self.transformation_code, transformation_file)


    def write_with_header(self):
        print "Begin the wirte with headers"
        with open(self.filetowrite) as input_file:
            with open(self.filetowrite + "_copy", "w") as outputfile:
                outputfile.write(self.delimiter.join(self.header) + "\n")
                for line in input_file:
                    outputfile.write(line)
        os.rename(self.filetowrite + "_copy",self.filetowrite)

if __name__ == "__main__":
    ch = Hoover("test_file/test.csv",",","test_file/output.csv", 1)
    ch.add_header(True)
    ch.add_type()
    #print ch.type_dict
    ch.add_transformation()
    #print ch.apply_line_transformation(ch.firstline)
    ch.apply_file_transformation()
    #ch.separate_into_chunks()
    #ch.apply_file_transformation_multiprocess()
    #ch.reassemblate_chunks()
