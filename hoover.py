import csv
import os
import copy
import multiprocessing

TYPE_DICT = {1 : "integer", 2 : "float", 3 : "string"}

def create_transformation_function(expression, type):

    def transformation(x):
        if type == "integer":
            x = int(x)
        if type == "float":
            x = float(x)
        result = eval(expression)
        return result

    return transformation

class Hoover(object):

    def __init__(self, filetoclean, delimiter, filetowrite,nb_process):
        self.filetoclean = filetoclean
        self.delimiter = delimiter
        self.filetowrite = filetowrite
        self.nb_process = nb_process

    def add_type(self):
        type_list = []
        with open(self.filetoclean) as input_file:
            reader = csv.reader(input_file, delimiter=self.delimiter)
            firstline = reader.next()
        self.firstline = firstline
        for key, type in TYPE_DICT.items():
            print "%s : %s" %(key, type)
        print "For each field, type the type of the field"
        for field in firstline:
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

    def add_transformation(self):
        transformation_list = []
        print "For each field x, print the transformation to apply to x"
        for i, field in enumerate(self.firstline):
            while True:
                print(str(field) + " : "),
                transformation_list.append(create_transformation_function(raw_input(), self.type_list[i]))
                try:
                    result = transformation_list[i](field)
                    print "The result will for this field will be %s, is this correct ? [y/n]" %(result)
                    agree = raw_input()
                    if agree == "y":
                        break
                    transformation_list.pop()
                except SyntaxError:
                    print "The function you write has an invalid syntax. Please retry"
                    transformation_list.pop()
        self.transformation_list = transformation_list

    def apply_line_transformation(self, line):
        line_result = []
        for i, field in enumerate(line):
            line_result.append(self.transformation_list[i](field))
        return line_result

    def apply_file_transformation(self):
        with open(self.filetoclean) as input_file:
            with open(self.filetowrite,"w") as output_file:
                reader = csv.reader(input_file, delimiter = self.delimiter)
                writer = csv.writer(output_file, delimiter = self.delimiter)
                for line in reader:
                    writer.writerow(self.apply_line_transformation(line))

    def separate_into_chunks(self):
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
        with open(self.filetowrite, "w") as output_file:
            for i in range(0, self.nb_process):
                with open(self.filetowrite + "_" + str(i)) as input_file:
                    for line in input_file:
                        output_file.write(line)
                os.remove(self.filetoclean + "_" + str(i))
                os.remove(self.filetowrite + "_" + str(i))

if __name__ == "__main__":
    ch = Hoover("test.csv",",","output.csv", 2)
    ch.add_type()
    #print ch.type_dict
    ch.add_transformation()
    #print ch.apply_line_transformation(ch.firstline)
    #ch.apply_file_transformation()
    ch.separate_into_chunks()
    ch.apply_file_transformation_multiprocess()
    ch.reassemblate_chunks()
