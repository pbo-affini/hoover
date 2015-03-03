# ./venv/bin/py.test to launch all tests

from hoover import *
import pytest
import mock
import os

@pytest.fixture()
def hoover():
    hoover = Hoover("test_file/input.csv", ",", "test_file/output.csv")
    return hoover

@pytest.fixture()
def hoover_multi():
    hoover = Hoover("test_file/input.csv", ",", "test_file/output.csv", 3)
    with mock.patch('__builtin__.raw_input', return_value='n'):
        hoover.add_header(False)
    with mock.patch('__builtin__.input', side_effect=[3, 1, 3, 1]):
        hoover.add_type()
    with mock.patch('__builtin__.raw_input', side_effect = ["x + \'test\'","y", "x * 2", "y"]):
        hoover.add_transformation(field_to_transform=[0, 1])
    return hoover

@pytest.yield_fixture()
def hoover_header():
    hoover = Hoover("test_file/input_with_header.csv", ",", "test_file/outputi_with_header.csv")
    yield hoover
    with open("test_file/input_with_header.csv", "w") as outputfile:
        outputfile.write("firstheader,secondheader\n1,2\n3,4")

@pytest.yield_fixture()
def hoover_complete():
    functions_dict = {"multiply()" : multiply}
    hoover = Hoover("test_file/input_with_header.csv", ",", "test_file/output_with_header.csv")
    hoover.add_header(True)
    with mock.patch('__builtin__.input', side_effect=[1 , 3]):
        hoover.add_type()
    with mock.patch('__builtin__.raw_input', side_effect = ["multiply()", "y", "x + '_test'", "y"]):
        hoover.add_transformation(functions_dict)
    yield hoover
    with open("test_file/input_with_header.csv", "w") as outputfile:
        outputfile.write("firstheader,secondheader\n1,2\n3,4")

@pytest.yield_fixture()
def hoover_error():
    functions_dict = {"multiply()" : multiply}
    hoover = Hoover("test_file/input_with_error.csv", ",", "test_file/output_with_error.csv")
    with mock.patch('__builtin__.raw_input', return_value='n'):
        hoover.add_header(False)
    with mock.patch('__builtin__.input', side_effect=[1 , 3]):
        hoover.add_type()
    with mock.patch('__builtin__.raw_input', side_effect = ["multiply()", "y", "x + '_test'", "y"]):
        hoover.add_transformation(functions_dict)
    yield hoover

def test_add_header(hoover_header, hoover):
    hoover_header.add_header(True)
    assert hoover_header.header == ["firstheader","secondheader"]
    assert hoover_header.first_line == ["1","2"]
    
    hoover.add_header(False, False, True)
    assert hoover.header == ["field 0","field 1", "field 2", "field 3"]
    assert hoover.first_line == ["t1","1","f1","1"]
    hoover.header = None

    with mock.patch('__builtin__.raw_input', side_effect=["f0","f1","f2","f3"]):
        hoover.add_header(False)
        assert hoover.header == ["f0","f1", "f2", "f3"]
    assert hoover.first_line == ["t1","1","f1","1"]
    hoover.write_infos(True, True, False, False)
    hoover.header = None
    hoover.first_line = None

    hoover.add_header(False,True)
    assert hoover.header == ["f0","f1", "f2", "f3"]
    assert hoover.first_line == ["t1","1","f1","1"]
    os.remove("test_file/input.csv_header")
    os.remove("test_file/input.csv_first_line")

def test_add_type(hoover_header):
    hoover_header.add_header(True)
    with mock.patch('__builtin__.input', side_effect=[1 , 3]):
        hoover_header.add_type()
        assert hoover_header.type_list == ["integer", "string"]
    hoover_header.write_infos(False, False, True, False)
    hoover_header.type_list = []
    hoover_header.add_type(True)
    assert hoover_header.type_list == ["integer", "string"]
    os.remove("test_file/input_with_header.csv_type_list")

def multiply(x):
    return x * 2

def test_add_transformation(hoover):
    functions_dict = {"multiply()" : multiply}
    with mock.patch('__builtin__.raw_input', side_effect = ["y","f0","f1","f2","f3"]):
        hoover.add_header(False)
    with mock.patch('__builtin__.input', side_effect=[3 , 1, 3, 1]):
        hoover.add_type()

    #test with a functions dict
    with mock.patch('__builtin__.raw_input', side_effect = ["x + \'test\'","y","multiply()","y","yexf","x + \'_test2\'","n","x","y","x","y"]):
        hoover.add_transformation(functions_dict)
    assert hoover.transformation_list[0]("field") == "fieldtest"
    assert hoover.transformation_list[1](2) == 4
    assert hoover.transformation_list[2]("field") == "field"
    assert hoover.transformation_list[3](2) == 2

    #test with file load
    hoover.write_infos(False, False, False, True)
    hoover.transformation_list = []
    hoover.add_transformation(file_transformation = True)
    print hoover.transformation_list[0]("field")
    assert hoover.transformation_list[0]("field") == "fieldtest"
    assert hoover.transformation_list[1](2) == 4
    assert hoover.transformation_list[2]("field") == "field"
    assert hoover.transformation_list[3](2) == 2
    os.remove("test_file/input.csv_transformation_code")

    #test without functions dict and with selected field
    hoover.transformation_list = []
    with mock.patch('__builtin__.raw_input', side_effect = ["x + \'test\'","y"]):
        hoover.add_transformation(field_to_transform=[0])
    assert hoover.transformation_list[0]("field") == "fieldtest"
    assert hoover.transformation_list[1](2) == 2
    assert hoover.transformation_list[2]("field") == "field"
    assert hoover.transformation_list[3](2) == 2

def test_apply_line_transformation(hoover_complete):
    line = [2 , "test"]
    assert hoover_complete.apply_line_transformation(line) == [4, "test_test"]

def test_apply_file_transformation(hoover_complete, hoover_error):
    hoover_complete.apply_file_transformation()
    assert os.path.exists("test_file/output_with_header.csv")
    assert not os.path.exists("test_file/output_with_header.csv_error")
    with open("test_file/output_with_header.csv") as output_file:
        lines = output_file.readlines()
        assert len(lines) == 2
        assert lines[0] == "2,2_test\n"
        assert lines[1] == "6,4_test\n"
    os.remove("test_file/output_with_header.csv")

    #test with errors
    hoover_error.apply_file_transformation()
    assert os.path.exists("test_file/output_with_error.csv")
    with open("test_file/output_with_error.csv") as output_file:
        lines = output_file.readlines()
        assert len(lines) == 3
        assert lines[0] == "2,2_test\n"
        assert lines[1] == "10,6_test\n"
        assert lines[2] == "14,8_test\n"
    assert os.path.exists("test_file/output_with_error.csv_error")
    with open("test_file/output_with_error.csv_error") as error_file:
        lines = error_file.readlines()
        assert len(lines) == 2
        assert lines[0] == "a,4\n"
        assert lines[1] == "b,10\n"
    os.remove("test_file/output_with_error.csv")
    os.remove("test_file/output_with_error.csv_error")

def test_separate_into_chunks(hoover_multi):
    hoover_multi.separate_into_chunks()
    assert os.path.exists("test_file/input.csv_0")
    assert os.path.exists("test_file/input.csv_1")
    assert os.path.exists("test_file/input.csv_2")
    with open("test_file/input.csv_0") as input_file:
        lines = input_file.readlines()
        assert len(lines) == 3
        assert lines[0] == "t1,1,f1,1\n"
        assert lines[1] == "t2,2,f2,2\n"
        assert lines[2] == "t2,a,f2,2\n"
    with open("test_file/input.csv_1") as input_file:
        lines = input_file.readlines()
        assert len(lines) == 3
        assert lines[0] == "t2,4,f2,2\n"
        assert lines[1] == "t2,5,f2,2\n"
        assert lines[2] == "t2,6,f2,2\n"
    with open("test_file/input.csv_2") as input_file:
        lines = input_file.readlines()
        assert len(lines) == 4
        assert lines[0] == "t2,7,f2,2\n"
        assert lines[1] == "t2,8,f2,2\n"
        assert lines[2] == "t2,c,f2,2\n"
        assert lines[3] == "t2,10,f2,2\n"
    os.remove("test_file/input.csv_0")
    os.remove("test_file/input.csv_1")
    os.remove("test_file/input.csv_2")

def test_apply_file_transformation_multiprocess(hoover_multi):
    hoover_multi.separate_into_chunks()
    hoover_multi.apply_file_transformation_multiprocess()
    assert os.path.exists("test_file/output.csv_0")
    assert os.path.exists("test_file/output.csv_1")
    assert os.path.exists("test_file/output.csv_2")
    assert os.path.exists("test_file/output.csv_0_error")
    assert os.path.exists("test_file/output.csv_2_error")
    with open("test_file/output.csv_0") as input_file:
        lines = input_file.readlines()
        assert len(lines) == 2
        assert lines[0] == "t1test,2,f1,1\n"
        assert lines[1] == "t2test,4,f2,2\n"
    with open("test_file/output.csv_1") as input_file:
        lines = input_file.readlines()
        assert len(lines) == 3
        assert lines[0] == "t2test,8,f2,2\n"
        assert lines[1] == "t2test,10,f2,2\n"
        assert lines[2] == "t2test,12,f2,2\n"
    with open("test_file/output.csv_2") as input_file:
        lines = input_file.readlines()
        assert len(lines) == 3
        assert lines[0] == "t2test,14,f2,2\n"
        assert lines[1] == "t2test,16,f2,2\n"
        assert lines[2] == "t2test,20,f2,2\n"
    os.remove("test_file/input.csv_0")
    os.remove("test_file/input.csv_1")
    os.remove("test_file/input.csv_2")
    os.remove("test_file/output.csv_0")
    os.remove("test_file/output.csv_1")
    os.remove("test_file/output.csv_2")
    os.remove("test_file/output.csv_0_error")
    os.remove("test_file/output.csv_2_error")

def test_reassemblate_chunks(hoover_multi):
    hoover_multi.separate_into_chunks()
    hoover_multi.apply_file_transformation_multiprocess()
    hoover_multi.reassemblate_chunks()
    assert not os.path.exists("test_file/input.csv_0")
    assert not os.path.exists("test_file/output.csv_0")
    assert not os.path.exists("test_file/output.csv_0_error")
    assert os.path.exists("test_file/output.csv")
    assert os.path.exists("test_file/output.csv_error")
    with open("test_file/output.csv") as output_file:
        lines = output_file.readlines()
        assert len(lines) == 8
        assert lines[0] == "t1test,2,f1,1\n"
        assert lines[1] == "t2test,4,f2,2\n"
        assert lines[2] == "t2test,8,f2,2\n"
        assert lines[3] == "t2test,10,f2,2\n"
        assert lines[4] == "t2test,12,f2,2\n"
        assert lines[5] == "t2test,14,f2,2\n"
        assert lines[6] == "t2test,16,f2,2\n"
        assert lines[7] == "t2test,20,f2,2\n"
    with open("test_file/output.csv_error") as error_file:
        lines = error_file.readlines()
        assert len(lines) == 2
        assert lines[0] == "t2,a,f2,2\n"
        assert lines[1] == "t2,c,f2,2\n"
    os.remove("test_file/output.csv")
    os.remove("test_file/output.csv_error")

def test_write_infos(hoover_complete):
    functions_dict = {"multiply()" : multiply}
    hoover_complete.write_infos(True, True, True, True)
    assert os.path.exists("test_file/input_with_header.csv_header")
    assert os.path.exists("test_file/input_with_header.csv_first_line")
    assert os.path.exists("test_file/input_with_header.csv_type_list")
    assert os.path.exists("test_file/input_with_header.csv_transformation_code")
    with open("test_file/input_with_header.csv_header") as header_file:
        assert json.load(header_file) == ["firstheader","secondheader"]
    with open("test_file/input_with_header.csv_first_line") as first_line_file:
        assert json.load(first_line_file) == ["1", "2"]
    with open("test_file/input_with_header.csv_type_list") as type_list_file:
        assert json.load(type_list_file) == ["integer", "string"]
    with open("test_file/input_with_header.csv_transformation_code") as transformation_list_file:
        assert json.load(transformation_list_file) == ["def multiply(x):\n    return x * 2\n", "x + '_test'"]
    os.remove ("test_file/input_with_header.csv_header")
    os.remove("test_file/input_with_header.csv_first_line")
    os.remove("test_file/input_with_header.csv_type_list")
    os.remove("test_file/input_with_header.csv_transformation_code")

def test_write_with_header(hoover_complete):
    hoover_complete.apply_file_transformation()
    hoover_complete.write_with_header()
    assert os.path.exists(hoover_complete.filetowrite)
    with open(hoover_complete.filetowrite) as output_file:
        lines = output_file.readlines()
        assert len(lines) == 3
        assert lines[0] == "firstheader,secondheader\n"
        assert lines[1] == "2,2_test\n"
        assert lines[2] == "6,4_test\n"
    os.remove(hoover_complete.filetowrite)
