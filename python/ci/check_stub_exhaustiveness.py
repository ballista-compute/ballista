#!/usr/bin/env python3
import sys
if sys.version_info.minor < 5:
    print("This script requires at least python 3.5")
import typing
import os

if not os.getcwd().endswith("ci"):
    os.chdir("ci")


import ast
import inspect


python_version = sys.version_info

pyi_file = "../ballista/__init__.pyi"


so_path = f"./ballista.so"

#There is a time of check time of use issue here, shouldn't matter so long as multiple tools aren't running simultaneously 
if not os.path.isfile(so_path):
    print("When checking type stub exhaustiveness require direct access to dynamic library, make sure dy-lib is in ballista/python/ci/ as 'ballista.so'")
    print("Possible fix is running 'maturin develop' or running this test through cargo make --makefile python-tasks.toml exhaust-stub")
    sys.exit(1)


def top_level_functions(body: typing.Any)->typing.Any:
    return (f for f in body if isinstance(f, ast.FunctionDef))

def parse_interface_ast(filename: str)->typing.Any:
    if filename[-4:] != ".pyi":
        raise Exception("The extension of the interface filename must be pyi found extension: {}".format(filename[-4:]))
    filename_with_py_ext = filename[:-4] + ".py"
    with open(filename,"rt") as f:
        ast.parse(f.read(), filename=filename_with_py_ext)



 

def is_pyo3_func_or_method(obj: typing.Any)->bool:
    return str(getattr(getattr(obj, "__class__"), "__name__")) == "builtin_function_or_method"

def is_pyo3_method(obj: typing.Any)->bool:
    class_name = str(getattr(getattr(obj,"__class__"), "__name__"))
    return class_name == "method_descriptor" or class_name == "builtin_function_or_method"


def is_pyo3_class(obj: typing.Any)->bool:
    return str(getattr(getattr(obj,"__class__"), "__name__")) == "type"

def print_attributes(obj: typing.Any)->None:
    for attr in dir(obj):
        print(f"{attr}: {getattr(obj, attr)}")


def has_rust_defined_new(obj: typing.Any)->bool:
    #Try calling __new__ with self as argument, if a TypeError with the exact message 'No constructor defined' is thrown then there is no defined rust constructor and  
    #__init__ type annotations are not required
    try:
        obj.__new__(obj)
    except TypeError as e:
        #print(f"{e}")
        if f"{e}" == "No constructor defined":
            return False
        else:
            return True
    except Exception:
        return True
    else:
        #If no exception is thrown then there is a rust defined constructor taking 0 arguments
        return True
    return True

def isprop(v: typing.Any)->bool:
  return isinstance(v, property)
    
def is_gettable_attr(obj: typing.Any)->bool:
    try:
        class_name: str = getattr(getattr(obj, "__class__"),"__name__")
        return class_name == "getset_descriptor"
    except:
        return False

def is_function(obj: typing.Any, type_stub: bool = False)->bool:
    if type_stub:
        return inspect.isfunction(obj)
    else:
        return is_pyo3_func_or_method(obj)

def is_class(obj: typing.Any, type_stub: bool = False)->bool:
    if type_stub:
        return inspect.isclass(obj)
    else:
        return is_pyo3_class(obj)


def is_method(obj: typing.Any, type_stub: bool = False)->bool:
    if type_stub:
        return inspect.isfunction(obj)
    else:
        return is_pyo3_method(obj)

def is_attr(obj: typing.Any, type_stub: bool = False)->bool:
    if type_stub:
        is_ballista_attr = False
        if "__ballista__" in dir(obj):
            is_ballista_attr = "attr" in str(getattr(obj, "__ballista__"))
            if is_ballista_attr:
                return True
        return inspect.isgetsetdescriptor(obj) 
    else:
        return is_gettable_attr(obj)

def ballista_noskip(obj: typing.Any)->bool:
    if "__ballista__" in dir(obj):
        return str(getattr(obj, "__ballista__")) != "skip"
    else:
        return True


class Class:
    def __init__(self, name: str, obj: typing.Any, type_stub: bool = False)->None:
        self.class_name = name
        self.methods: typing.Set[str] = set()
        self.attributes: typing.Set[str] = set()
        #If there is a defined 
        if not type_stub and has_rust_defined_new(obj):
            self.methods.add("__init__")
        #print(f"{name} has rust defined __new__: {has_rust_defined_new(obj)}")
        #print(name)
        #print_attributes(obj)
        #print("\n\n")

        for member_name, data in inspect.getmembers(obj, ballista_noskip):
            #If creating a representation of the type_stub then check if the __init__ method has type annotations
            if type_stub and member_name == "__init__":
                if "__annotations__" in dir(data):
                    self.methods.add("__init__")
                #print(getattr(data, "__text_signature__"))
            if member_name.startswith("__"):
                continue
            #print(f"{member_name} has attributes:\n")
            #print_attributes(data)
            #print("\n\n")
            data_is_method = is_method(data, type_stub=type_stub)
            data_is_attribute = is_attr(data, type_stub=type_stub)
            if data_is_method and not data_is_attribute:
                self.methods.add(member_name)
            elif data_is_attribute and not data_is_method:
                self.attributes.add(member_name)
            elif data_is_method and data_is_attribute:
                raise Exception("Found item which was method and attribute at same time")
           
    def add_method(self, method_name: str)->None:
        self.methods.add(method_name)
    
    def add_attributes(self, attribute_name: str)->None:
        self.attributes.add(attribute_name)

    def __str__(self, depth:int=0)->str:
        ntabs: typing.Callable[[int], str] = lambda n: "\t"*n
        return "{}{}:\n{}attributes: [{}]\n{}methods: [{}]".format(ntabs(depth), self.class_name, ntabs(depth+1),",".join(self.attributes), ntabs(depth+1),",".join(self.methods))





class Module:
    def __init__(self, module_name:str, obj:typing.Any, type_stub: bool = False)->None:
        self.name = module_name
        self.functions: typing.Set[str] = set()
        self.submodules: typing.Dict[str,'Module'] = {}
        self.classes: typing.Dict[str, Class]={}
        self.attributes: typing.Set[str] = set()
        for name, data in inspect.getmembers(obj, ballista_noskip):
            is_private = name.startswith("_")
            if is_private:
                continue

            data_is_function = is_function(data, type_stub=type_stub)
            data_is_class = is_class(data, type_stub=type_stub)
            if data_is_function and not data_is_class:
                self.add_function(name)
            elif data_is_class and not data_is_function:
                self.add_class(Class(name, data, type_stub=type_stub))
            elif inspect.ismodule(data):
                self.add_submodule(Module(name, data, type_stub=type_stub))
            else:
                self.add_attributes(name)
        #print(f"TypeStub[{type_stub}: {self.attributes}")


    def add_function(self, func_name: str)->None:
        if func_name in self.functions:
            raise Exception(f"Module {self.name} already had a function named {func_name}")
        self.functions.add(func_name)
    
    def add_submodule(self, module: 'Module')->None:
        if self.submodules.get(module.name) is not None:
            raise Exception(f"Module {self.name} already had a submodule {module.name}")
        self.submodules[module.name] = module

    def add_class(self, cls: Class)->None:
        if self.classes.get(cls.class_name) is not None:
            raise Exception(f"Module {self.name} already had a class named {cls.class_name}")
        self.classes[cls.class_name] = cls

    def add_attributes(self, attribute_name: str)->None:
        if attribute_name in self.attributes:
            raise Exception(f"Module {self.name} already had a attribute/property named {attribute_name}")
        self.attributes.add(attribute_name)








    
    def __str__(self, depth: int=0)->str:
        ntabs: typing.Callable[[int], str] = lambda n: "\t"*n
        if len(self.classes) == 0:
            classes_str = "{}classes: {{}}\n".format(ntabs(depth+1))
        else:
            classes_str = "{}classes: {{\n{} }}\n".format(ntabs(depth+1),  ",\n".join("{}".format(c.__str__(depth=depth+2)) for c in self.classes.values())) #type: ignore

        if len(self.attributes) == 0:
            attributes_str = "{}attributes: []\n".format(ntabs(depth+1))
        else:
            attributes_str= "{}attributes: [\n{} ]\n".format(ntabs(depth+1), ",\n".join("{}{}".format(ntabs(depth+2),a) for a in self.attributes) )
        if len(self.submodules) == 0:
            submodules_str = "{}modules:[]\n".format(ntabs(depth+1))
        else:
            submodules_str ="{}modules: [\n{} ]\n".format(ntabs(depth+1), ",\n".join("{}".format(m.__str__(depth=depth+2)) for m in self.submodules.values())) #type: ignore
        str_repr = f"{ntabs(depth)}name:{self.name}\n{classes_str}{attributes_str}{submodules_str}"

        return str_repr


class ClassMatch:
    def __init__(self, actual: Class, interface: Class, qualified_name: str):
        if actual.class_name != interface.class_name:
            raise Exception("Attempted to class match on classes with different names: Actual['{}'] Interface['{}']".format(actual.class_name, interface.class_name))
        self.attributes_ani = actual.attributes - interface.attributes
        self.attributes_ina = interface.attributes - actual.attributes

        self.qualified_name = qualified_name + "." + actual.class_name
        self.methods_ani = actual.methods - interface.methods
        self.methods_ina = interface.methods - actual.methods

        #print(self.qualified_name)
        #print(actual.methods)
        #print(interface.methods)
        #print("Class matching attributes: ")
        #print(self.attributes_ani)
        #print(self.attributes_ina)
        #print("\n\n")
        #print(self.methods_ani)
        #print(self.methods_ina)

        
    def __bool__(self)->bool:
        return len(self.attributes_ani) + len(self.attributes_ina) + len(self.methods_ani) + len(self.methods_ina) ==0

    def add_errors(self, errors: typing.List[str])->None:
        #print(self.attributes_ina)
        #print(self.attributes_ani)
        #print(self.methods_ani)
        #print(self.methods_ina)
        for ani_attr in self.attributes_ani:
            errors.append(f"In {self.qualified_name} found the attribute {ani_attr} in the actual implementation and not in the interface. Add definition to ballista.pyi")
        for ina_attr in self.attributes_ina:
            errors.append(f"In {self.qualified_name} found the attribute {ina_attr} in the interface and not in the implementation, check that attribute was not removed from the class.")
        for ani_method in self.methods_ani:
            errors.append(f"In {self.qualified_name} found the method {ani_method} in the actual implementation and not in the interface. Add definition to ballista.pyi")
        for ina_method in self.methods_ina:
            errors.append(f"In {self.qualified_name} found the method {ina_method} in the interface and not in the implementation. Was this method moved or removed")




    
class ModuleMatch:
    def __init__(self, actual: Module, interface: Module, qualified_name: str=""):
        if actual.name != interface.name:
            raise Exception("Attempted to module match on modules with different names: Actual['{}'] Interface['{}']".format(actual.name, interface.name))
        if qualified_name == "":
            self.qualified_name = actual.name
        else:
            self.qualified_name = qualified_name + "."+actual.name
        self.attributes_ani = actual.attributes - interface.attributes
        self.attributes_ina = interface.attributes - actual.attributes

        
        self.functions_ani = actual.functions - interface.functions
        self.functions_ina = interface.functions - actual.functions
        self.submodule_matches: typing.List['ModuleMatch'] = []
        
        actual_module_name_set = set(actual.submodules.keys())
        interface_module_name_set = set(interface.submodules.keys())
        self.module_names_ani = actual_module_name_set - interface_module_name_set
        self.module_names_ina = interface_module_name_set - actual_module_name_set
        
        for name, mod in actual.submodules.items():
            if name not in self.module_names_ani and name not in self.module_names_ina:
                self.submodule_matches.append(ModuleMatch(mod, interface.submodules[name], self.qualified_name))
            else:
                continue
    
        actual_class_name_set = set(actual.classes.keys())
        interface_class_name_set = set(interface.classes.keys())
        self.class_names_ani = actual_class_name_set - interface_class_name_set
        self.class_names_ina = interface_class_name_set - actual_class_name_set

        self.class_matches: typing.List[ClassMatch] = []
        for name, clss in actual.classes.items():
            if name not in self.class_names_ani and name not in self.class_names_ina:
                self.class_matches.append(ClassMatch(clss, interface.classes[name], self.qualified_name))
            else:
                continue

    def add_errors(self, errors: typing.List[str])->None:
        for ani_attr in self.attributes_ani:
            errors.append(f"In module {self.qualified_name} found the attribute {ani_attr} in the actual implementation and not in the interface. Add attribute definition to type stub")
        for ina_attr in self.attributes_ina:
            errors.append(f"In module {self.qualified_name} found the attribute {ina_attr} in the interface and not in the implementation, check that attribute was not removed from module.")
        for ani_func in self.functions_ani:
            errors.append(f"In module {self.qualified_name} found the function {ani_func} in the actual implementation and not in the interface. Add function definition to type stub")
        for ina_func in self.functions_ina:
            errors.append(f"In module {self.qualified_name} found the function {ina_func} in the interface and not in the implementation, check that function was not removed from module.")
        for ani_module in self.module_names_ani:
            errors.append(f"In module {self.qualified_name} expected to find the module {ani_module} in the implementation and not in the interface. Add module definition type stub.")
        for ina_module in self.module_names_ina:
            errors.append(f"In module {self.qualified_name} expected to find the module {ina_module} in the interface and not in the implementation. Check that module was not removed or renamed.")
        for ani_class in self.class_names_ani:
            errors.append(f"In module {self.qualified_name} found the class {ani_class} in the actual implementation and not in the interface. Add class definition to type stub")
        for ina_class in self.class_names_ina:
            errors.append(f"In module {self.qualified_name} found the attribute {ina_class} in the interface and not in the implementation, check that class was not removed from module.")
        for class_match in self.class_matches:
            class_match.add_errors(errors)
        for module_match in self.submodule_matches:
            module_match.add_errors(errors)







with open(pyi_file, "rt") as f:
    ballista_typestub = f.read()


#Apparently there is some weirdness involving importlib's submodules, and they become initialized after a failed access
#https://stackoverflow.com/questions/39660934/error-when-using-importlib-util-to-check-for-library
try:
    import importlib.util
    spec = importlib.util.spec_from_loader("ballista_typecheck", loader = None) #type: ignore
except:
    spec = importlib.util.spec_from_loader("ballista_typecheck", loader = None) #type: ignore




try:
    import ballista

    #mypy doesn't acknowledge the existence of importlib.util
    spec = importlib.util.spec_from_loader("ballista_typecheck", loader = None) #type: ignore
    ballista_typecheck = importlib.util.module_from_spec(spec) #type: ignore
    exec(ballista_typestub, ballista_typecheck.__dict__)
    sys.modules["ballista_typecheck"] = ballista_typecheck

    #Since the native module import is in __init_.py to refer to the native module directly use ballista.ballista
    ballista_module = Module("ballista", ballista)
    ballista_typecheck_module = Module("ballista", ballista_typecheck, type_stub=True)
    errors: typing.List[str] = []
    match = ModuleMatch(ballista_module, ballista_typecheck_module)
except Exception as e:
    print("Coud not complete typecheck successfully: {}", e)
    sys.exit(1)
match.add_errors(errors)
if len(errors) == 0:
    print("All good!")
    sys.exit(0)
    pass
else:
    print("There were mismatches found between implementation and interface definition: \n")
    for err in errors:
        print("\t{}".format(err))
    sys.exit(1)
