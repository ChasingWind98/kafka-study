package de.thi.example.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;

@Data
@NoArgsConstructor
public class Employee {
    private String empNo;
    private String name;
    private String department;
    private int age;
    private double salary;
    private String title;


    public Employee(Builder builder) {
        this.empNo = builder.empNo;
        this.name = builder.name;
        this.department = builder.department;
        this.age = builder.age;
        this.salary = builder.salary;
        this.title = builder.title;

    }


    @NoArgsConstructor
    public static class Builder {
        private String empNo;
        private String name;
        private String department;
        private int age;
        private double salary;
        private String title;

        public static Builder newBuilder(Employee employee) {
            Builder builder = new Builder();
            builder.empNo = employee.empNo;
            builder.name = employee.name;
            builder.department = employee.department;
            builder.age = employee.age;
            builder.salary = employee.salary;
            builder.title = employee.title;
            return builder;
        }

        public Builder empNo(String empNo) {
            this.empNo = empNo;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder department(String department) {
            this.department = department;
            return this;
        }

        public Builder age(int age) {
            this.age = age;
            return this;
        }

        public Builder salary(double salary) {
            this.salary = salary;
            return this;
        }

        public Builder title(String title) {
            this.title = title;
            return this;
        }

        public Builder evaluateTitle(){
            if (this.salary >=  10000){
                this.title = "Senior " + this.title;
            }
            else if (this.salary < 10000 && this.salary >= 5000){
                this.title = "MEDIUM " + this.title;
            }else{
                this.title = "Junior " + this.title;
            }
            return this;
        }

        public Employee build() {
            return new Employee(this);
        }

    }


    public static void main(String[] args) throws JsonProcessingException {
        Employee employee = new Builder()
                .empNo("E1001")
                .name("Tom")
                .department("IT")
                .age(25)
                .salary(5000)
                .title("Software Engineer")
                .build();

        Employee employee1 = new Builder()
                .empNo("E1002")
                .name("Alex")
                .department("HR")
                .age(30)
                .salary(100001)
                .title("HR")
                .build();
        Employee employee2 = new Builder()
                .empNo("E1003")
                .name("Jina")
                .department("Security")
                .age(67)
                .salary(100001)
                .title("Security")
                .build();

        System.out.println(new ObjectMapper().writeValueAsString(employee));
        System.out.println(new ObjectMapper().writeValueAsString(employee1));
        System.out.println(new ObjectMapper().writeValueAsString(employee2));
    }
}
