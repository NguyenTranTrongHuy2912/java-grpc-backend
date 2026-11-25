package iuh.fit.grpc.service;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import iuh.fit.grpc.chat.SearchUsersRequest;
import iuh.fit.grpc.chat.SearchUsersResponse;
import iuh.fit.grpc.chat.UserSummary;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.regex.Pattern;

import static iuh.fit.grpc.service.Rpc.fail;

/** Tìm người dùng theo tên hoặc theo email (hỗ trợ cả chữ hoa, chữ thường và tìm theo tiền tố/hậu tố). */
public class UserSearchService {
    private static final Logger log = LoggerFactory.getLogger(UserSearchService.class);

    private static final int MIN_SEARCH_LENGTH = 2;
    private static final int MAX_SEARCH_LENGTH = 100;
    private static final int DEFAULT_SEARCH_LIMIT = 10;
    private static final int MAX_SEARCH_LIMIT = 20;

    private final MongoCollection<Document> userCollection;

    public UserSearchService(MongoDatabase db) {
        this.userCollection = db.getCollection("users");
    }

    private static String makeVnRegex(String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toLowerCase(Locale.ROOT).toCharArray()) {
            switch (c) {
                case 'a' -> sb.append("[aàáảãạăằắẳẵặâầấẩẫậ]");
                case 'e' -> sb.append("[eèéẻẽẹêềếểễệ]");
                case 'i' -> sb.append("[iìíỉĩị]");
                case 'o' -> sb.append("[oòóỏõọôồốổỗộơờớởỡợ]");
                case 'u' -> sb.append("[uùúủũụưừứửữự]");
                case 'y' -> sb.append("[yỳýỷỹỵ]");
                case 'd' -> sb.append("[dđ]");
                case '.', '^', '$', '*', '+', '?', '(', ')', '[', '{', '\\', '|' -> sb.append('\\').append(c);
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    public void searchUsers(SearchUsersRequest request, StreamObserver<SearchUsersResponse> out) {
        String requesterId = request.getRequesterId().trim();
        String query = request.getQuery().trim();
        if (requesterId.isEmpty()) {
            fail(out, Status.INVALID_ARGUMENT.withDescription("Thiếu requester_id"));
            return;
        }
        if (query.length() < MIN_SEARCH_LENGTH || query.length() > MAX_SEARCH_LENGTH) {
            fail(out, Status.INVALID_ARGUMENT.withDescription(
                    "Từ khóa phải từ " + MIN_SEARCH_LENGTH + " đến " + MAX_SEARCH_LENGTH + " ký tự"));
            return;
        }
        int limit = request.getLimit() <= 0 ? DEFAULT_SEARCH_LIMIT
                : Math.min(request.getLimit(), MAX_SEARCH_LIMIT);

        try {
            String qLower = query.toLowerCase(Locale.ROOT);
            String qVn = makeVnRegex(query);
            // Thuật toán tìm kiếm thông minh: Tìm đồng thời cả Tên hiển thị (hỗ trợ tiếng Việt không dấu) và Email/Account
            Bson match = Filters.or(
                    Filters.regex("account", Pattern.quote(qLower), "i"),
                    Filters.regex("display_name", qVn, "i")
            );

            SearchUsersResponse.Builder builder = SearchUsersResponse.newBuilder();
            for (Document u : userCollection
                    .find(Filters.and(match, Filters.ne("is_active", false)))
                    .projection(Projections.include("display_name", "account"))
                    .sort(Sorts.ascending("display_name"))
                    .limit(limit)) {
                String dName = u.getString("display_name");
                String acc = u.getString("account");
                builder.addUsers(UserSummary.newBuilder()
                        .setUserId(u.getString("_id"))
                        .setDisplayName(dName != null ? dName : (acc != null ? acc.split("@")[0] : "Người dùng"))
                        .setEmail(acc != null ? acc : ""));
            }
            out.onNext(builder.build());
            out.onCompleted();
        } catch (Exception e) {
            log.error("Tìm người dùng thất bại", e);
            fail(out, Status.INTERNAL.withDescription("Không tìm được người dùng"));
        }
    }
}
