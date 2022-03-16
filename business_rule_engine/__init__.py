from inspect import signature
import logging
from collections import OrderedDict
import pprint
import formulas  # type: ignore
import asyncio
#from methods_lib import *
from business_rule_methods import *
import sys

from typing import (
    Any,
    Dict,
    List,
    Text,
    Tuple,
    Optional
)

from business_rule_engine.exceptions import (
    DuplicateRuleName,
    MissingArgumentError,
    ConditionReturnValueError
)

PREFETCH_PREFIX = 'get_'

def parse_action_prefetch_params (string):
    logging.debug("params string: %s for parsing", string)
    parsed_string = string.split('(') #separate method name from params
    method_name = parsed_string[0].strip()
    params = parsed_string[1] 
    params = params.replace(' ', '')
    params = params.replace(')','').split(',') #remove last ) and split string to list
    return {'method_name': method_name, 'params': params}

def parse_prefetch_params (string):
    def remove_constants(list):
        filtered_list = []
        numbers = ['0','1','2', '3', '4', '5', '6', '7', '8', '9', 'true', 'false', '(', ')']
        for item in list:
            flag = False
            for number in numbers:
                if number in item:
                    flag = True
                    break
            if flag == False:
                filtered_list.append(item)
        return filtered_list
            
    tokens = string.split(' ')
    operators = ['<', '>', '<=', '>=', '=', '', '+', '-', '/', '*']
    filtered_tokens = [token for token in tokens if token not in operators]
    param_names = remove_constants(filtered_tokens)
    return param_names

class Rule():
    def __init__(self, rulename, condition_requires_bool: bool = True) -> None:
        self.condition_requires_bool = condition_requires_bool
        self.rulename: Text = rulename
        self.conditions: List[Text] = []
        self.actions: List[Text] = []
        self.status = None

    @staticmethod
    def _compile_condition(condition_lines: List[Text]) -> Any:
        condition = "".join(condition_lines)
        if not condition.startswith("="):
            condition = "={}".format(condition)
        return formulas.Parser().ast(condition)[1].compile()  # type: ignore

    @staticmethod
    async def run_actions(self, action_and_params, event_context):
        action_results = []
        for action in self.actions:
            exploded_action_list = action.split('(')
            action_name = exploded_action_list[0]
            this_action_params = None
            for action in action_and_params:
                if action_name == action ['method_name']:
                    this_action_params = action ['param_values']
            try:
                result = await globals()[action_name](rule_name=self.rulename, parameters=this_action_params, previous_actions_results=action_results, event_context=event_context)
            except KeyError as e:
                raise KeyError (e)

            action_response = {}
            action_response ['action_name'] = action_name
            action_response ['action_context'] = result ['action_context']
            if ('halt_actions' in result and 'action_context' in result):
                if result['halt_actions'] == True:
                    action_results.append(action_response)
                    break
                else:
                    action_results.append(action_response)
            else:
                # return error and stop further actions
                # if action returns errornoeus response 
                return ValueError
        return action_results

    @staticmethod
    def _get_params(params: Dict[Text, Any], condition_compiled: Any, set_default_arg: bool = False, default_arg: Optional[Any] = None) -> Dict[Text, Any]:
        params_dict: Dict[Text, Any] = {k.upper(): v for k, v in params.items()}
        param_names = set(params_dict.keys())

        condition_args: List[Text] = list(condition_compiled.inputs.keys())

        if not set(condition_args).issubset(param_names):
            missing_args = set(condition_args).difference(param_names)
            if not set_default_arg:
                raise MissingArgumentError("Missing arguments {}".format(missing_args))

            for missing_arg in missing_args:
                params_dict[missing_arg] = default_arg

        params_condition = {k: v for k, v in params_dict.items() if k in condition_args}
        return params_condition

    def check_condition(self, params, *, set_default_arg=False, default_arg=None):
        condition_compiled = self._compile_condition(self.conditions)
        params_condition = self._get_params(params, condition_compiled, set_default_arg, default_arg)
        rvalue_condition = condition_compiled(**params_condition).tolist()
        if self.condition_requires_bool and not isinstance(rvalue_condition, bool):
            raise ConditionReturnValueError('rule: {} - condition does not return a boolean value!'.format(self.rulename))
        self.status = bool(rvalue_condition)
        return rvalue_condition

    async def execute(self, condition_params, action_params, event_context, *, set_default_arg=False, default_arg=None) -> Tuple[bool, Any]:
        rvalue_condition = self.check_condition(condition_params, set_default_arg=set_default_arg, default_arg=default_arg)
        if not self.status:
            return rvalue_condition, None
        rvalue_action = await self.run_actions(self, action_params, event_context)
        return rvalue_condition, rvalue_action


class RuleParser():

    CUSTOM_FUNCTIONS: List[Text] = []

    def __init__(self, condition_requires_bool: bool = True) -> None:
        self.rules: Dict[Text, Rule] = OrderedDict()
        self.condition_requires_bool = condition_requires_bool

    def parsestr(self, text: Text) -> None:
        rulename = None
        is_condition = False
        is_action = False
        ignore_line = False

        for line in text.split('\n'):
            ignore_line = False
            line = line.strip()  # The split on rule name doesn't work for multi-line w/o
            if line.lower().startswith('rule'):
                is_condition = False
                is_action = False
                rulename = line.split(' ', 1)[1].strip("\"")
                if rulename in self.rules: #not useful in parallel impl
                    raise DuplicateRuleName("Rule '{}' already exists!".format(rulename))
                self.rules[rulename] = Rule(rulename)
            if line.lower().strip().startswith('when'):
                ignore_line = True
                is_condition = True
                is_action = False
            if line.lower().strip().startswith('then'):
                ignore_line = True
                is_condition = False
                is_action = True
            if line.lower().strip().startswith('end'):
                ignore_line = True
                is_condition = False
                is_action = False
            if rulename and is_condition and not ignore_line:
                self.rules[rulename].conditions.append(line.strip())
            if rulename and is_action and not ignore_line:
                self.rules[rulename].actions.append(line.strip())
        all_rules_have_condition = True
        all_rules_have_action = True
        for rule in self.rules:
            if len(self.rules[rule].conditions) == 0:
                all_rules_have_condition = False
            if len(self.rules[rule].actions) == 0:
                all_rules_have_action = False
        if all_rules_have_condition == False:
            raise TypeError ("some rules have missing condition")
        if all_rules_have_action == False:
            raise TypeError ("some rules have missing action")



    @classmethod
    def register_function(cls, function: Any, function_name: Optional[Text] = None) -> None:
        cls.CUSTOM_FUNCTIONS.append(function_name or function.__name__.upper())
        formulas.get_functions()[function_name or function.__name__.upper()] = function  # type: ignore

    def __iter__(self):
        return self.rules.values().__iter__()

class RulesEngine():
    async def parse_rule(self, rule, local_conditions, local_params, event_context):
        async def fetch_param_values(parameters):
            param_dict = {}
            for param in parameters:
                if param not in local_params:
                    try:
                        param_value = int (param)
                        param_dict[param] = param_value
                    except ValueError:
                        try:
                            if '"' not in param:
                                if PREFETCH_PREFIX+param not in globals():
                                    raise KeyError ("prefetch methods: {} not implemented".format(param))
                                else:
                                    try:
                                        param_value = await globals()[PREFETCH_PREFIX+param](event_context)
                                        param_dict[param] = param_value
                                    except Exception as e:
                                        raise e
                            else:
                                param_value = param[1:-1]
                                param_dict[param] = param_value
                        except Exception as e:
                            raise e
            return param_dict

        async def fetch_action_param_values(parameters, local_params, event_context):
            params = []
            for param in parameters:
                if param not in local_params:
                    try:
                        param_value = int (param)
                        params.append(param_value)
                    except ValueError:
                        try:
                            if '"' not in param:
                                param_value = await globals()[PREFETCH_PREFIX+param](event_context)
                            else:
                                param_value = param[1:-1]
                                params.append(param_value)
                        except KeyError:
                            logging.debug("Method: %s not implemented", param)
            return params

        parser = RuleParser()
        for function in local_conditions:
            parser.register_function(function)

        parser.parsestr(rule)
        for rule in parser:
            action_params = []
            for action in rule.actions:
                parsed_action_params = parse_action_prefetch_params(action)
                param_names = parsed_action_params ['params']
                method_name = parsed_action_params ['method_name']
                param_values = await fetch_action_param_values(param_names, local_params, event_context)
                action_params.append ({'param_values': param_values, 'method_name': method_name})

            condition_param_names = parse_prefetch_params(rule.conditions[0])
            condition_params = await fetch_param_values (condition_param_names)
            condition_params.update(local_params) #add local params

            rvalue_condition, rvalue_action = await rule.execute(condition_params, action_params, event_context)
            if rule.status:
                return ({
                        'rule_name':rule.rulename,
                        'condition':rvalue_condition,
                        'actions':rvalue_action
                    })
            else:
                return ({
                        'rule_name':rule.rulename,
                        'condition':rvalue_condition,
                        'actions':[]
                    })
                    
    async def run_engine(self, rules, local_conditions, local_params, event_context):
        if isinstance(rules, list):
            if (len(rules) < 1):
                raise ValueError ("specify at least one rule")
        else:
            raise TypeError ("rule must be of type list")
        
        for condition in local_conditions:
            if not callable (condition):
                raise TypeError ("local conditions must be callable")
        tasks_list = []
        for rule in rules:
            try:
                task = asyncio.create_task(self.parse_rule (rule, local_conditions, local_params, event_context))
                tasks_list.append(task)
            except Exception as e:
                raise Exception (str(e))
        rule_results = []
        for task in tasks_list:
            result = await asyncio.gather (task)
            rule_results.append(result[0])

        return rule_results
