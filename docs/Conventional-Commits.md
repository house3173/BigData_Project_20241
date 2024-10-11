# Hướng dẫn sử dụng Conventional Commits

## 1. Cấu trúc chung của Commit Message

Một commit message sẽ bao gồm 3 phần chính:

```plaintext
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

- **Type**: Loại của commit, có thể là một trong các giá trị sau:
  - **build**: Thay đổi liên quan đến quá trình build hoặc các công cụ hỗ trợ
  - **ci**: Thay đổi liên quan đến quá trình CI/CD
  - **docs**: Thay đổi liên quan đến tài liệu
  - **feat**: Thêm một chức năng mới
  - **fix**: Sửa một lỗi
  - **perf**: Cải thiện hiệu suất
  - **refactor**: Thay đổi mà không sửa lỗi hoặc thêm chức năng
  - **style**: Cải thiện code style
  - **test**: Thêm hoặc sửa các test
  - **chore**: Các thay đổi không liên quan đến mã nguồn, ví dụ: cấu hình, quản lý dự án, v.v.
  - **revert**: Hoàn tác một commit trước đó
- **Scope**: Phạm vi của commit, có thể bỏ qua
- **Description**: Mô tả ngắn gọn về nội dung của commit
- **Body**: Nội dung chi tiết hơn về commit (không bắt buộc)
- **Footer**: Thông tin bổ sung, ví dụ: liên kết issue, breaking change (không bắt buộc)

## 2. Các loại commit chính và ví dụ

### 2.1. feat: Thêm tính năng mới

Sử dụng khi bạn thêm tính năng mới cho dự án.

```plaintext
feat(<scope>): <description>
```

Ví dụ:

```plaintext
feat(auth): add JWT-based authentication
```

### 2.2. fix: Sửa lỗi

Sử dụng khi bạn sửa một lỗi trong mã nguồn.

```plaintext
fix(<scope>): <description>
```

Ví dụ:

```plaintext
fix(api): correct user profile data fetching issue
```

### 2.3. docs: Cập nhật tài liệu

Sử dụng khi bạn cập nhật hoặc chỉnh sửa tài liệu.

```plaintext
docs(<scope>): <description>
```

Ví dụ:

```plaintext
docs(README): add installation instructions for Docker setup
```

### 2.4. style: Sửa đổi định dạng mã

Sử dụng khi bạn thay đổi định dạng hoặc phong cách mã (như chỉnh khoảng trắng, dấu phẩy, không ảnh hưởng logic mã).

```plaintext
style(<scope>): <description>
```

Ví dụ:

```plaintext
style(ui): fix button spacing in header
```

### 2.5. refactor: Tái cấu trúc mã

Sử dụng khi bạn thay đổi cấu trúc mã mà không thay đổi hành vi của mã.

```plaintext
refactor(<scope>): <description>
```

Ví dụ:

```plaintext
refactor(database): optimize user query logic
```

### 2.6. perf: Cải thiện hiệu suất

Sử dụng khi bạn thực hiện thay đổi để cải thiện hiệu suất hệ thống.

```plaintext
perf(<scope>): <description>
```

Ví dụ:

```plaintext
perf(api): reduce response time for large datasets
```

### 2.7. test: Thêm hoặc sửa test

Sử dụng khi bạn thêm hoặc cập nhật test cases.

```plaintext
test(<scope>): <description>
```

Ví dụ:

```plaintext
test(auth): add unit tests for login endpoint
```

### 2.8. chore: Thay đổi không liên quan đến mã nguồn hoặc tính năng

Sử dụng cho các thay đổi nhỏ, như cập nhật dependencies, thay đổi cấu hình dự án.

```plaintext
chore(<scope>): <description>
```

Ví dụ:

```plaintext
chore: update npm dependencies
```

### 2.9. build: Thay đổi hệ thống build

Sử dụng khi thay đổi các công cụ build hoặc cấu hình CI/CD.

```plaintext
build(<scope>): <description>
```

Ví dụ:

```plaintext
build(docker): add Dockerfile for continuous integration
```

### 2.10. ci: Thay đổi liên quan đến CI/CD

Sử dụng khi thay đổi cấu hình hệ thống tích hợp liên tục (CI/CD).

```plaintext
ci(<scope>): <description>
```

Ví dụ:

```plaintext
ci(github): configure GitHub Actions for automated testing
```

### 2.11. revert: Hoàn tác một commit

Sử dụng khi bạn cần hoàn tác một commit trước đó.

```plaintext
revert: <description>
```

Ví dụ:

```plaintext
revert: revert commit 9f1234abc that broke auth logic
```

## 3. Merge Commits

### 3.1. Merge Commit Tự Động

Khi bạn thực hiện một merge mà không cần thay đổi gì, Git sẽ tự động tạo commit message:

```plaintext
Merge branch '<branch-name>' into '<target-branch>'
```

Ví dụ:

```plaintext
Merge branch 'feature/auth' into 'main'
```

### 3.2. Merge Commit Tùy Chỉnh

Nếu bạn muốn tùy chỉnh commit message khi merge, bạn có thể sử dụng merge làm type để diễn đạt rõ ràng hơn.

```plaintext
merge(<source-branch>): <description>
```

Ví dụ:

```plaintext
merge(feature/cart): resolve conflicts in cart and checkout logic
```

### 3.3. Merge Với Giải Quyết Xung Đột

Khi merge mà cần giải quyết xung đột, bạn có thể ghi rõ lý do và phạm vi thay đổi.

```plaintext
merge(<source-branch>): resolve conflicts in <scope>
```

Ví dụ:

```plaintext
merge(feature/payment): resolve conflicts in payment gateway integration
```

## 4. Breaking Changes (Thay Đổi Phá Vỡ)

Nếu commit của bạn tạo ra những thay đổi phá vỡ tính tương thích ngược (breaking changes), bạn cần thêm vào footer để người khác biết về sự thay đổi này.

```plaintext
BREAKING CHANGE: <description>
```

Ví dụ:

```plaintext
refactor(auth): change token format to JWT

BREAKING CHANGE: The token format has been changed to JWT. Existing sessions will be invalidated.
```

## 5. Footer: Đóng Issue Hoặc PR

Nếu commit của bạn đóng một issue hoặc PR, bạn có thể thêm liên kết đến nó trong footer:

```plaintext
Closes #<issue-number>
```

Ví dụ:

```plaintext
fix(api): handle missing data in user profiles

Closes #123
```

## Lời khuyên cuối cùng

- Giữ commit nhỏ và rõ ràng: Mỗi commit chỉ nên giải quyết một thay đổi nhỏ để dễ dàng theo dõi.
- Chia commits theo loại: Phân loại commit giúp bạn quản lý và tìm kiếm dễ dàng hơn trong lịch sử thay đổi.
- Sử dụng footer: Đừng quên liên kết đến các issue hoặc PR liên quan, cũng như ghi chú các thay đổi phá vỡ (breaking changes) nếu có.

Áp dụng các quy tắc trên sẽ giúp dự án của bạn dễ dàng quản lý, đồng thời tạo ra lịch sử commit dễ đọc và rõ ràng cho cả đội ngũ phát triển.